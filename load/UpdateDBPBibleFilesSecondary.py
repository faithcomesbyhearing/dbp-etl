# UpdateDBPBibleFilesSecondary

import sys
import boto3
from Log import Log
from Config import Config
from SQLUtility import SQLUtility
from SQLBatchExec import SQLBatchExec
from AWSSession import AWSSession
from S3Utility import S3Utility
from S3ZipperService import S3ZipperService
from BibleBrainService import BibleBrainService, Package

class UpdateDBPBibleFilesSecondary:


	def __init__(self, config, db, dbOut, languageReader):
		self.config = config
		self.db = db 
		self.dbOut = dbOut
		self.languageReader = languageReader

		s3_client = AWSSession.shared().s3Client

		self.zipper_service = S3ZipperService(
			s3zipper_user_key=self.config.s3_zipper_user_key,
			s3zipper_user_secret=self.config.s3_zipper_user_secret,
			s3_client=s3_client,
			region=self.config.s3_aws_region
		)

		self.biblebrain_service = BibleBrainService(s3_client=s3_client, base_url=self.config.biblebrain_services_base_url)


	def createAllZipFiles(self, inputFilesetList):
		for inp in inputFilesetList:
			self.createZipFile(inp)


	def createZipFile(self, inputFileset):
		inp = inputFileset
		# Ensure AWS Session initialization
		s3 = S3Utility(self.config)

		logger = Log.getLogger(inp.filesetId)

		# Handle audio filesets related to the MP3 fileset and the process to generate the zip file
		if inp.typeCode == "audio" and inp.isMP3Fileset() and len(inp.filesetId) == 10:
			# We need to get the list of files that are stored in the database
			# and are associated with the filesetId plus the files that are being loaded
			files = self.languageReader.list_existing_and_loaded_files(inp)
			complete_files = [
				f"{self.config.s3_bucket}/{inp.filesetPrefix}/{f}"
				for f in files
			]

			if not complete_files:
				print(f"No files found for fileset {inp.filesetId}. Skipping zip creation.")
				return

			try:
				# Attempt to fetch and upload the PDF for the filesetId
				# This will raise RuntimeError if the product code is not found or if the upload fails
				package_id = self._fetch_and_upload_pdf(
					fileset_id=inp.filesetId,
					bible_id=inp.bibleId,
					book_id=None,  # No book_id for audio filesets
					type_code=inp.typeCode,
					bucket=self.config.s3_bucket,
					prefix=inp.filesetPrefix
				)
				complete_files.append(f"{self.config.s3_bucket}/{inp.filesetPrefix}/{package_id}.pdf")
				print(f"Uploaded PDF for fileset {inp.filesetId} → {package_id}.pdf")
			except RuntimeError as e:
				print(f"PDF step skipped for {inp.filesetId}: {e}")

			try:
				# Attempt to create a zip file with the complete files
				zip_filename = f"{inp.filesetPrefix}/{inp.filesetId}.zip"
				result = self.zipper_service.zip_files(self.config.s3_bucket, complete_files, zip_filename)
				if result.get("State") != "SUCCESS":
					logger.message(Log.EROR, f"Failure creating Zip: {result}")
				else:
					inp.addInputFile(zip_filename, 0)
					print(f"Zip created: {zip_filename}\n")
			except Exception as e:
				logger.message(Log.EROR, f"Error creating Zip: {e}")

		# Handle video filesets related to the Gospel Films and the process to generate the zip file
		if inp.typeCode == "video" and inp.isMP4Fileset() and len(inp.filesetId) == 10:
			# We need to get the list of books that are allowed for the video fileset (Gospels and Apostolic History)
			booksAllowed = self.languageReader.getGospelsAndApostolicHistoryBooks()
			logger.message(Log.INFO, f"Creating Zip for {inp.filesetId} and creating temp credentials")
			print(f"Creating Zip for {inp.filesetId} and creating temp credentials key: {self.config.s3_zipper_user_key}")

			def create_zip_for_fileset(book_id, files):
				if not files:
					return

				logger.message(
					Log.INFO,
					f"Creating Zip for {book_id} with {len(files)} mp4 files in bucket {self.config.s3_vid_bucket}"
				)

				# Construct full S3 keys for each file, updating .mp4 filenames to include '_web'
				# We can assume that the transcoder has already created the _web.mp4 files
				# and they are in the same bucket as the original files.
				# Example: "s3://bucket/prefix/file.mp4" becomes "s3://bucket/prefix/file_web.mp4"
				complete_files = [
					f"{self.config.s3_vid_bucket}/{inp.filesetPrefix}/{f[:-4] + '_web.mp4' if f.endswith('.mp4') else f}"
					for f in files
				]

				try:
					# Attempt to fetch and upload the PDF for the filesetId
					# This will raise RuntimeError if the product code is not found or if the upload fails
					package_id = self._fetch_and_upload_pdf(
						fileset_id=inp.filesetId,
						bible_id=inp.bibleId,
						book_id=book_id,
						type_code=inp.typeCode,
						bucket=self.config.s3_vid_bucket,
						prefix=inp.filesetPrefix
					)
					complete_files.append(f"{self.config.s3_vid_bucket}/{inp.filesetPrefix}/{package_id}.pdf")
					print(f"Uploaded PDF for fileset {inp.filesetId} → {package_id}.pdf")
				except RuntimeError as e:
					print(f"PDF step skipped for {inp.filesetId}: {e}")

				# Validate that each S3 key exists in the video bucket.
				# If the key does not exist, log a warning. However, the zip file creation should
				# not fail if a file is missing and it should still proceed to create the zip file
				# with the available files.
				for s3key in complete_files:
					key = s3key.split('/', 1)[1]
					exists, _ = s3.get_key_info(self.config.s3_vid_bucket, key)
					if not exists:
						print(f"\nWARN: Creating Zip Video File - Missing s3 key: {key}")

				try:
					zip_filename = f"{inp.filesetPrefix}/{inp.filesetId}_{book_id}.zip"
					result = self.zipper_service.zip_files(self.config.s3_vid_bucket, complete_files, zip_filename)
					if result.get("State") != "SUCCESS":
						logger.message(Log.EROR, f"Failure creating Zip: {result}")
					else:
						inp.addInputFile(zip_filename, 0)
						print(f"Zip created: {zip_filename}\n")
				except Exception as e:
					logger.message(Log.EROR, f"Error creating Zip: {e}")

			if inputFileset.hasGospelAndActFilmsVideo(booksAllowed):
				# Create a zip file for each book in the allowed list
				# The zip file will contain the _web.mp4 files for each book.
				for bookId in booksAllowed:
					files = inp.videoFileNamesByBook(bookId)
					create_zip_for_fileset(bookId, files)
			else:
				cov_book_id = self.languageReader.getCovenantBookId()
				files = inputFileset.videoCovenantFileNames()
				create_zip_for_fileset(cov_book_id, files)


	def _fetch_and_upload_pdf(self, fileset_id: str, bible_id: str, type_code: str, book_id: str|None, bucket: str, prefix: str) -> str:
		"""
		Calculates the product code, fetches the PDF, and uploads it to S3.
		Returns the package ID (sans .pdf).
		Raises RuntimeError on any failure.
		"""
		languageRecord, _ = self.languageReader.getLanguageRecordLoose(
			type_code, bible_id, fileset_id
		)

		product_code = languageRecord.CalculateProductCode(fileset_id, type_code, book_id)

		if not product_code:
			raise RuntimeError("No product code returned")

		# Build S3 key
		# For now, we will use a fixed package_id (copyright) for copyright PDFs
		package_id = "copyright"
		pdf_key = f"{prefix}/{package_id}.pdf"

		# Fetch & upload
		self.biblebrain_service.fetch_and_upload(
			product_codes=[product_code],
			mode=type_code.lower(),
			format="pdf",
			bucket_name=bucket,
			key=pdf_key,
			extra_args={"ACL": "bucket-owner-full-control"}
		)
		return package_id

	def getZipInternalDir(self, damId, languageRecord):
		langName = languageRecord.LangName()
		iso3 = languageRecord.ISO()
		stockNo = languageRecord.Reg_StockNumber()
		versionCode = stockNo[-3:] if stockNo != None else ""
		scope = damId[6]
		if scope == "N" or scope == "O":
			scope += "T"
		style = "Drama" if damId[7] == "2" else "Non-Drama"
		return "%s_%s_%s_%s_%s" % (langName, iso3, versionCode, scope, style)


	def updateBibleFilesSecondary(self, hash_id, input_fileset):
		"""
		Syncs the bible_files_secondary table for this fileset:
		- For audio: art, zip, thumbnail
		- For video: zip
		Inserts any missing (hash_id, file_name, file_type) rows.
		"""
		type_code = input_fileset.typeCode

		# Build a map file_type -> list of file-name‑or‑file‑obj
		file_type_map = {}

		if type_code == "audio":
			file_type_map['art'] = input_fileset.artFiles()
			file_type_map['thumbnail'] = input_fileset.thumbnailFiles()
			zip_file = input_fileset.zipFile()
			if zip_file:
				# store the object, so we can call only_file_name() below
				file_type_map['zip'] = [zip_file]

		if type_code == "video":
			zip_files_map = input_fileset.zipFilesIndexedByBookId()
			# Fetch just the Gospel book IDs
			gospel_book_name_map = self.languageReader.getGospelsAndApostolicHistoryBooks()
			# Filter your zip_files_map to only those keys in gospel_book_ids
			zip_files_by_book_id = [
				zip_file
				for book_id, zip_file in zip_files_map.items()
				if book_id in gospel_book_name_map
			]
			if zip_files_by_book_id:
				file_type_map['zip'] = zip_files_by_book_id

			# If the zip file is a covenant file, add it to the list
			if input_fileset.zipFile() and self.languageReader.getCovenantBookId() in input_fileset.zipFile().only_file_name():
				zip_files_by_covenant_id = input_fileset.zipFile()
				file_type_map['zip'] = [zip_files_by_covenant_id]

		if not file_type_map:
			return

		sql = "SELECT file_name FROM bible_files_secondary WHERE hash_id = %s AND file_type = %s"

		insert_rows = []
		# For each file_type, check if the file_name exists in the db
		# If not, add it to the insert_rows list
		for file_type, candidates in file_type_map.items():
			if not candidates:
				continue

			existing_names = self.db.selectSet(sql, (hash_id, file_type))

			for candidate in candidates:
				# if it's an object with only_file_name(), use that; else assume it's already a string
				name = (
					candidate.only_file_name()
					if hasattr(candidate, 'only_file_name')
					else candidate
				)

				if name not in existing_names:
					insert_rows.append((file_type, hash_id, name))

		if insert_rows:
			# Insert the new rows into the bible_files_secondary table
			# table_name = "bible_files_secondary"
			# pkey_names = ("hash_id", "file_name")
			# attr_names = ("file_type",)
			self.dbOut.insert("bible_files_secondary", ("hash_id", "file_name"), ("file_type",), insert_rows)


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from PreValidate import *
	from InputFileset import *

	if len(sys.argv) < 4:
		print("Usage: UpdateDBPBibleFilesSecondary.py  your_profile  starting_bible_id   ending_bible_id")
		sys.exit()
	startingBibleId = sys.argv[2]
	endingBibleId = sys.argv[3]

	config = Config.shared()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	update = UpdateDBPBibleFilesSecondary(config, db, dbOut, languageReader)
	s3Client = AWSSession.shared().s3Client

	sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
			" WHERE f.hash_id = c.hash_id AND set_type_code in ('audio', 'audio_drama') AND length(f.id) = 10"
			" AND c.bible_id >= %s AND c.bible_id <= %s")
	resultSet = db.select(sql, (startingBibleId, endingBibleId))
	for (bibleId, filesetId, hashId) in resultSet:
		print(bibleId, filesetId, hashId)
		location = "s3://%s" % (config.s3_bucket,)
		filesetPath = "audio/%s/%s" % (bibleId, filesetId)
		(dataList, messages) = PreValidate.validateDBPETL(s3Client, location, filesetId, filesetPath, None)
		if messages != None and len(messages) > 0:
			Log.addPreValidationErrors(messages)
			#print(filesetPath, messages)
		if dataList == None or len(dataList) == 0:
			print("NO InputFileset", filesetPath)
		else:
			for data in dataList:
				#print(data.toString())
				inp = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
								data.typeCode, data.bibleId(), data.index, data.languageRecord)
				#print(inp.toString())
				if inp.zipFile() == None:
					print("must create zip file")
					update.createZipFile(inp)
				update.updateBibleFilesSecondary(hashId, inp)
				dbOut.displayStatements()
				dbOut.displayCounts()
				dbOut.execute("zip_art_thumbnail_" + filesetId)
	Log.writeLog(config)

# python3 load/UpdateDBPBibleFilesSecondary.py test starting_bible_id  ending_bible_id

"""
CREATE TABLE bible_files_secondary (
  hash_id VARCHAR(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  file_name VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  file_type VARCHAR(12) NOT NULL,
  CHECK (file_type IN ('art', 'font', 'zip')),
  PRIMARY KEY (hash_id, file_name),
  CONSTRAINT FK_bible_filesets_bible_files_secondary FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`)
)

"""
