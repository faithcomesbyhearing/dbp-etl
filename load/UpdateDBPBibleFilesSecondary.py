# UpdateDBPBibleFilesSecondary

import sys
import boto3
import json
from Log import *
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from AWSSession import *
from S3ZipperService import S3ZipperService

class UpdateDBPBibleFilesSecondary:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db 
		self.dbOut = dbOut


	def createAllZipFiles(self, inputFilesetList):
		for inp in inputFilesetList:
			self.createZipFile(inp)


	def createZipFile(self, inputFileset):
		inp = inputFileset
		if inp.typeCode == "audio" and inp.isMP3Fileset() and len(inp.filesetId) == 10:
			source = "s3://%s/%s/" % (self.config.s3_bucket, inp.filesetPrefix)
			zipFilename = "%s.zip" % (inp.filesetId,)
			target = "s3://%s/%s/%s" % (self.config.s3_bucket, inp.filesetPrefix, zipFilename)
			zipDirectory = self.getZipInternalDir(inp.lptsDamId, inp.languageRecord)
			fileTypes = ["mp3"]
			data = { "source": source, "target": target, "directoryName": zipDirectory, "fileTypes": fileTypes }
			print(data)
			dataJson = json.dumps(data)
			dataBytes = dataJson.encode("utf-8")
			lambdaClient = AWSSession.shared().lambdaInvoker()
			try:
				response = lambdaClient.invoke(
													FunctionName = self.config.lambda_zip_function,
													InvocationType = "RequestResponse",
													Payload = dataBytes)
				respData = response.get("Payload").read()
				respStr = respData.decode("utf-8")
				result = json.loads(respStr)
				status = result.get("status")
				inp.addInputFile(zipFilename, 0) # size is unknown and not needed
				if status != "SUCCESS":
					result = json.loads(result.get("body"))
					Log.getLogger(inp.filesetId).message(Log.EROR, "Error in zip file creation %s" % (result.get("error"),))
			except Exception as err:
				Log.getLogger(inp.filesetId).message(Log.EROR, "Failure in zip file creation %s" % (err,))
		# Handle video filesets related to the Gospel Films and the process to generate the zip file
		if inp.typeCode == "video" and inp.isMP4Fileset() and len(inp.filesetId) == 10:
			gospelBookNameMap = self.db.selectMap("SELECT id, notes FROM books where book_group = 'Gospels'", None)
			AWSSession.shared() # ensure AWSSession init
			session = boto3.Session(profile_name = self.config.s3_aws_profile)
			s3Client = session.client('sts', region_name=self.config.s3_aws_region)
			Log.getLogger(inp.filesetId).message(Log.INFO, "Creating Zip for %s and creating temp credentials" % (inp.filesetId,))
			print("Creating Zip for %s and creating temp credentials key: %s secret: %s" % (inp.filesetId, self.config.s3_zipper_user_key, self.config.s3_zipper_user_secret))
			zipperService = S3ZipperService(s3zipper_user_key=self.config.s3_zipper_user_key, s3zipper_user_secret=self.config.s3_zipper_user_secret, sts_client=s3Client, region=self.config.s3_aws_region)

			for bookId in gospelBookNameMap.keys():
				listFiles = inp.videoFileNamesByBook(bookId)
				if len(listFiles) > 0:
					Log.getLogger(inp.filesetId).message(Log.INFO, "Creating Zip for %s and # of mp4: %s bucket %s" % (bookId, len(listFiles), self.config.s3_vid_bucket))

					listCompleteFiles = [ "%s/%s/%s" % (self.config.s3_vid_bucket, inp.filesetPrefix, f) for f in listFiles ]
					try:
						zipFilename = "%s/%s_%s.zip" % (inp.filesetPrefix, inp.filesetId, bookId)
						result = zipperService.zip_files(self.config.s3_vid_bucket, listCompleteFiles, zipFilename)
						if result.get("State") != "SUCCESS":
							Log.getLogger(inp.filesetId).message(Log.EROR, "Failure creating Zip: %s" % (result,))
						else:
							inp.addInputFile(zipFilename, 0)
							Log.getLogger(inp.filesetId).message(Log.INFO, "Zip created: %s" % (zipFilename,))
					except Exception as e:
						Log.getLogger(inp.filesetId).message(Log.EROR, "Error creating Zip: %s" % (e,))

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


	def updateBibleFilesSecondary(self, hashId, inputFileset):
		inp = inputFileset
		insertRows = []

		sql = "SELECT file_name FROM bible_files_secondary WHERE hash_id = %s AND file_type = %s"
		if inp.typeCode == "audio":

			dbpArtSet = self.db.selectSet(sql, (hashId, 'art'))
			for artFile in inp.artFiles():
				if not artFile in dbpArtSet:
					insertRows.append(('art', hashId, artFile))

			zipFile = inp.zipFile()
			if zipFile != None:
				dbpZipSet = self.db.selectSet(sql, (hashId, 'zip'))
				if not zipFile.name in dbpZipSet:
					insertRows.append(('zip', hashId, zipFile.name))

			dbpThumbnailSet = self.db.selectSet(sql, (hashId, 'thumbnail'))
			for thumbnailFile in inp.thumbnailFiles():
				if not thumbnailFile in dbpThumbnailSet:
					insertRows.append(('thumbnail', hashId, thumbnailFile))


		tableName = "bible_files_secondary"
		pkeyNames = ("hash_id", "file_name")
		attrNames = ("file_type",)
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


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
	update = UpdateDBPBibleFilesSecondary(config, db, dbOut)
	s3Client = AWSSession.shared().s3Client
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)

	sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
			" WHERE f.hash_id = c.hash_id AND set_type_code in ('audio', 'audio_drama') AND length(f.id) = 10"
			" AND c.bible_id >= %s AND c.bible_id <= %s")
	resultSet = db.select(sql, (startingBibleId, endingBibleId))
	for (bibleId, filesetId, hashId) in resultSet:
		print(bibleId, filesetId, hashId)
		location = "s3://%s" % (config.s3_bucket,)
		filesetPath = "audio/%s/%s" % (bibleId, filesetId)
		(dataList, messages) = PreValidate.validateDBPETL(s3Client, location, filesetId, filesetPath)
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
