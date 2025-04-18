# UpdateDBPBiblesTable.py
#
# This program inserts and replaces records in the DBP database
# 1. bible_filesets
# 2. bible_files
#
# 1. Read validate/accepted csv files in this bucket process.
# 2. For each csv file in the directory, create a bible_filesets record
# 3. For each row in the csv file, create a bible_files record
# 4. Insert the transactions one fileset at a time.
# 5. I am going to use a command line batch using pipe, but for now, I can just use pymysql

import re
import sys
import csv
import os
import math
import subprocess
from SqliteUtility import SqliteUtility
from S3Utility import S3Utility
from Config import Config
from SQLUtility import SQLUtility
from SQLBatchExec import SQLBatchExec
from InputFileset import InputFileset
from UpdateDBPTextFilesets import UpdateDBPTextFilesets
from UpdateDBPBooksTable import UpdateDBPBooksTable
from UpdateDBPBibleFilesSecondary import UpdateDBPBibleFilesSecondary
from UpdateDBPLPTSTable import UpdateDBPLPTSTable
from UpdateDBPLicensorTables import UpdateDBPLicensorTables


class UpdateDBPFilesetTables:

	## Deuterocanon DC - the counts for OT to determine if complete should be aware that DC books may be involved.
	## do we consistently classify the DC books as DC and not OT
	def getSetSizeCode(NTBooks, OTBooks):
		hasNT = len(NTBooks)
		hasOT = len(OTBooks)
		if hasNT >= 27:
			if hasOT >= 39:
				return "C"
			elif hasOT > 0:
				return "NTOTP"
			else:
				return "NT"

		elif hasNT > 0:
			if hasOT >= 39:
				return "OTNTP"
			elif hasOT > 0:
				return "NTPOTP"
			else:
				return "NTP"

		else:
			if hasOT >= 39:
				return "OT"
			elif hasOT > 0:
				return "OTP"
			else:
				return "S"


	def __init__(self, config, db, dbOut, languageReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.statements = []
		self.rejectStatements = []
		self.OT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'OT'", ())
		self.NT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'NT'", ())
		self.durationRegex = re.compile(r"duration=([0-9\.]+)", re.MULTILINE)
		self.textUpdater = UpdateDBPTextFilesets(self.config, self.db, self.dbOut)
		self.booksUpdater = UpdateDBPBooksTable(self.config, self.dbOut)
		self.languageReader = languageReader



	def processFileset(self, inputFileset):
		inp = inputFileset
		print(inp.typeCode, inp.bibleId, inp.filesetId)
		lptsDBP = UpdateDBPLPTSTable(self.config, self.dbOut, self.languageReader)

		dbConn = SQLUtility(self.config)
		bookIdSet = self.getBibleBooks(inp.typeCode, inp.csvFilename, inp.databasePath)
		updateBibleFilesSecondary = UpdateDBPBibleFilesSecondary(self.config, dbConn, self.dbOut)
		updateLicensor = UpdateDBPLicensorTables(dbConn, self.dbOut)
		bucket = self.config.s3_vid_bucket if inp.typeCode == "video" else self.config.s3_bucket
		# it needs to know if the new inputFileset has new files to set the flag content_loaded
		isContentLoaded = 1 if len(inp.files) > 0 else 0

		if inp.typeCode in {"audio", "video"}:
			setTypeCode = inp.getSetTypeCode()
			hashId = lptsDBP.getHashId(bucket, inp.filesetId, setTypeCode)
			setSizeCode = self.getSizeCode(dbConn, inp.typeCode, hashId, bookIdSet)
			lptsDBP.upsertBibleFileset(dbConn, setTypeCode, setSizeCode, inp.filesetId, isContentLoaded)
			lptsDBP.upsertBibleFilesetConnection(dbConn, hashId, inp.bibleId)
			self.insertBibleFiles(dbConn, hashId, inputFileset, bookIdSet)
			updateBibleFilesSecondary.updateBibleFilesSecondary(hashId, inp)

			filesetList = []
			filesetList.append((inp.bibleId, inp.filesetId, setTypeCode, None, None, hashId))
			lptsDBP.updateBibleFilesetTags(filesetList)

			# update the bible_fileset_tags (product code) table with the new filesetId
			lptsDBP.updateBibleProductCode(inp, hashId)

			if inp.isDerivedFileset():
				updateLicensor.processFileset(inp.lptsDamId, hashId)
				# We need to create a license group for the derived audio fileset
				lptsDBP.updateBibleFilesetLicenseGroup(inp, hashId=hashId)

		elif inp.typeCode == "text":
			hashId = lptsDBP.getHashId(bucket, inp.filesetId, inp.subTypeCode())
			setSizeCode = self.getSizeCode(dbConn, inp.typeCode, hashId, bookIdSet)
			lptsDBP.upsertBibleFileset(dbConn, inp.subTypeCode(), setSizeCode, inp.filesetId, isContentLoaded)
			lptsDBP.upsertBibleFilesetConnection(dbConn, hashId, inp.bibleId)
			# text_plain is still stored in the database; no upload 
			if inp.subTypeCode() == "text_plain":
				self.textUpdater.updateFilesetTextPlain(inp.bibleId, inp.filesetId, hashId, bookIdSet, inp.databasePath)
				filesetList = []
				filesetList.append((inp.bibleId, inp.filesetId, inp.subTypeCode(), None, None, hashId))
				lptsDBP.updateBibleFilesetTags(filesetList)
				lptsDBP.updateBibleFilesetLicenseGroup(inp, hashId=hashId)
			elif inp.subTypeCode() in {"text_usx", "text_json"}:
				## The below code assumes Sofria-client has run. bookIdSet comes from Sofria-client. 
				self.insertBibleFiles(dbConn, hashId, inputFileset, bookIdSet)
				updateBibleFilesSecondary.updateBibleFilesSecondary(hashId, inp)
				filesetList = []
				filesetList.append((inp.bibleId, inp.filesetId, inp.subTypeCode(), None, None, hashId))
				lptsDBP.updateBibleFilesetTags(filesetList)
				lptsDBP.updateBibleFilesetLicenseGroup(inp, hashId=hashId)
			else:
				print("typeCode is text, but subTypeCode (%s) is not recognized. No hashId available to return, so it's going to fail next" % (inp.subTypeCode()))

			if inp.isDerivedFileset():
				updateLicensor.processFileset(inp.lptsDamId, hashId)

		tocBooks = self.booksUpdater.getTableOfContents(inp.typeCode, inp.bibleId, inp.filesetId, inp.csvFilename, inp.databasePath)
		self.booksUpdater.updateBibleBooks(inp.typeCode, inp.bibleId, tocBooks)
		
		dbConn.close()
		return hashId


	def getBibleBooks(self, typeCode, csvFilename, databasePath):
		bookIdSet = set()
		if typeCode == "text":
			sqlite = SqliteUtility(databasePath)
			# tableContents entity must have been populated by sofria-client. And the code means book code value
			bookIdSet = sqlite.selectSet("SELECT distinct code FROM tableContents", ())
			sqlite.close()
		else:
			with open(csvFilename, newline='\n') as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					bookIdSet.add(row["book_id"])
		return bookIdSet


	def getSizeCode(self, dbConn, typeCode, hashId, bookIdSet):
		if typeCode == "text":
			existingBookIdSet = dbConn.selectSet("SELECT book_id FROM bible_verses WHERE hash_id = %s", (hashId,))
		else:
			existingBookIdSet = dbConn.selectSet("SELECT book_id FROM bible_files WHERE hash_id = %s", (hashId,))			
		fullBookIdSet = existingBookIdSet.union(bookIdSet)
		otBooks = fullBookIdSet.intersection(self.OT)
		ntBooks = fullBookIdSet.intersection(self.NT)
		return UpdateDBPFilesetTables.getSetSizeCode(ntBooks, otBooks)



	def insertBibleFiles(self, db_conn, hash_id, input_fileset, book_id_set):
		"""
		Syncs bible_files rows for a given fileset:
		1. SELECT existing rows for this hash_id + book_ids
		2. Delegate to audio/video handler to get (inserts, updates, deletes)
		3. Batch apply inserts, updates, deletes via dbOut
		"""
		# 1) Fetch existing rows
		book_ids = list(book_id_set)
		placeholders = ",".join(["%s"] * len(book_ids))
		sql = (
			"SELECT book_id, chapter_start, verse_start, chapter_end, verse_end, "
			"file_name, file_size, duration "
			f"FROM bible_files WHERE hash_id = %s AND book_id IN ({placeholders})"
		)
		params = [hash_id] + book_ids
		existing_rows = db_conn.select(sql, tuple(params))

		# 2) Delegate to the right handler
		if input_fileset.typeCode == "audio":
			inserts, updates, deletes = self.handle_input_files_for_audio(existing_rows, hash_id, input_fileset)
		elif input_fileset.typeCode == "video":
			inserts, updates, deletes = self.handle_input_files_for_video(existing_rows, hash_id, input_fileset)

		# 3) Apply in batches
		table = "bible_files"
		pkeys = ("hash_id", "book_id", "chapter_start", "verse_start")
		attrs = ("chapter_end", "verse_end", "file_name", "file_size", "duration", "verse_sequence")

		self.dbOut.insert(table, pkeys, attrs, inserts, keyPosition=2)
		self.dbOut.updateCol(table, pkeys, updates)
		self.dbOut.delete(table, pkeys, deletes)

	def handle_input_files_for_audio(self, resultSet, hashId, inputFileset):
		insertRows = []
		updateRows = []
		dbpMap = {}
		for row in resultSet:
			(dbpBookId, dbpChapterStart, dbpVerseStart, dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration) = row
			key = (dbpBookId, dbpChapterStart, dbpVerseStart)
			value = (dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration)
			dbpMap[key] = value
		with open(inputFileset.csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				(chapterStart, chapterEnd, verseStart, verseSequence, verseEnd, fileSize) = self.parse_chapter_verse(row)
				fileName = row["file_name"]

				inputFile = inputFileset.getInputFile(fileName)
				duration = inputFile.duration if inputFile != None else None
				if inputFileset.locationType == InputFileset.LOCAL:
					duration = self.getDuration(inp.fullPath() + os.sep + fileName)
				key = (row["book_id"], chapterStart, verseStart)
				dbpValue = dbpMap.get(key)
				if dbpValue == None:
					insertRows.append((chapterEnd, verseEnd, fileName, fileSize, duration, verseSequence,
						hashId, row["book_id"], chapterStart, verseStart))
				else:
					del dbpMap[key]
					(dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration) = dbpValue
					# primary keys: "hash_id", "book_id", "chapter_start", "verse_start"
					if chapterEnd != dbpChapterEnd:
						updateRows.append(("chapter_end", chapterEnd, dbpChapterEnd, hashId, row["book_id"], chapterStart, verseStart))
					if verseEnd != dbpVerseEnd:
						updateRows.append(("verse_end", verseEnd, dbpVerseEnd, hashId, row["book_id"], chapterStart, verseStart))
					if fileName != dbpFileName:
						updateRows.append(("file_name", fileName, dbpFileName, hashId, row["book_id"], chapterStart, verseStart))
					if fileSize != dbpFileSize:
						updateRows.append(("file_size", fileSize, dbpFileSize, hashId, row["book_id"], chapterStart, verseStart))
					if duration != dbpDuration and duration != None and duration != "":
						updateRows.append(("duration", duration, dbpDuration, hashId, row["book_id"], chapterStart, verseStart))

		return (insertRows, updateRows, [])

	def handle_input_files_for_video(self, existing_rows, hash_id, input_fileset):
		"""
		For video filesets:
		- existing_rows: list of tuples from SELECT
		- build a map:  (book_id, chap_start, verse_start, extension) -> (chap_end, verse_end, filename, size, duration)
		- walk the CSV, generate three variants per row, check S3, then decide insert/update
		- any leftover keys in dbp_map get deleted
		"""
		s3 = S3Utility(self.config)

		# 1) Build lookup of existing DB rows with correct 'web_' prefix handling
		dbp_map = {}
		for book_id, dbp_chapter_start, dbp_verse_start, dbp_chapter_end, dbp_verse_end, dbp_file_name, dbp_size, dpb_dur in existing_rows:
			# get extension
			ext = dbp_file_name.rsplit(".", 1)[-1]
			if "web" in dbp_file_name:
				ext = f"web_{ext}"
			key = (book_id, dbp_chapter_start, dbp_verse_start, ext)
			dbp_map[key] = (dbp_chapter_end, dbp_verse_end, dbp_file_name, dbp_size, dpb_dur)

		inserts, updates = [], []
		seen_keys = set()

		with open(input_fileset.csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				c_start, c_end, v_start, v_seq, v_end, f_size = self.parse_chapter_verse(row)
				base = row["file_name"]
				prefix = input_fileset.filesetPrefix # e.g. "video/BIBLE_ID/FILESET_ID"

				# 3) Define the three variants
				variants = [
					("_stream.m3u8", "m3u8"),
					("_web.mp4",    "web_mp4"),
					(".mp4",        "mp4"),
				]

				for suffix, ext_key in variants:
					stem = base[:-4] if base.lower().endswith(".mp4") else base
					filename = f"{stem}{suffix}"
					s3_key  = f"{self.config.s3_vid_bucket}/{prefix}/{filename}"

					if not s3.IsKeyValid(self.config.s3_vid_bucket, f"{prefix}/{filename}"):
						continue

					key = (row["book_id"], c_start, v_start, ext_key)
					seen_keys.add(key)

					dbp = dbp_map.get(key)
					input_file = input_fileset.getInputFile(s3_key)
					duration = getattr(input_file, "duration", None) or (dbp[4] if dbp else None)

					if not dbp:
						inserts.append((
							c_end, v_end, filename, f_size, duration, v_seq,
							hash_id, row["book_id"], c_start, v_start
						))
					else:
						dbp_c_end, dbp_v_end, dbp_filename, dbp_size, dpb_dur = dbp

						# primary keys: "hash_id", "book_id", "chapter_start", "verse_start"
						if c_end   != dbp_c_end:
							updates.append(("chapter_end", c_end, dbp_c_end, hash_id, row["book_id"], c_start, v_start))
						if v_end   != dbp_v_end:
							updates.append(("verse_end", v_end, dbp_v_end, hash_id, row["book_id"], c_start, v_start))
						if filename != dbp_filename:
							updates.append(("file_name", filename, dbp_filename, hash_id, row["book_id"], c_start, v_start))
						if f_size  != dbp_size:
							updates.append(("file_size", f_size, dbp_size, hash_id, row["book_id"], c_start, v_start))
						if duration != dpb_dur and duration != None and duration != "":
							updates.append(("duration", duration, dpb_dur, hash_id, row["book_id"], c_start, v_start))

				# 4) Anything left in dbp_map wasn’t seen → delete
				deletes = [
					(hash_id, book_id, c_start, v_start)
					for (book_id, c_start, v_start, _) in dbp_map
					if (book_id, c_start, v_start, _) not in seen_keys
				]

		return inserts, updates, deletes

	def parse_chapter_verse(self, row):
		"""
		Normalize CSV row values into canonical ints/strings or None:
		- chapter_start, chapter_end: ints or None
		- verse_start, verse_end: strings or None
		- verse_sequence: int
		- file_size: int or None
		Handles the special “end of book” case when chapter_start == 'end'.
		"""
		def to_int(x, default=None):
			return int(x) if x and x.isdigit() else default

		book_id = row["book_id"]
		if row["chapter_start"] == "end":
			chap_start, _, _ = self.convertChapterStart(book_id)
			verse_start = row["verse_start"] or "0"
			verse_seq   = to_int(row["verse_sequence"], 0)
			if verse_seq:
				verse_start = str(verse_seq)
			chap_end = chap_start
			verse_end = verse_start
		else:
			chap_start   = to_int(row["chapter_start"])
			chap_end     = to_int(row["chapter_end"])
			verse_start  = row["verse_start"]  or "1"
			verse_end    = row["verse_end"]    or None
			verse_seq    = to_int(row["verse_sequence"], 1)

		file_size = to_int(row["file_size"])
		return chap_start, chap_end, verse_start, verse_seq, verse_end, file_size

	# We have a fixed values for chapterStart, verseStart, verseEnd for the following books
	# MAT 28:21,21
	# MRK 16:21,21
	# LUK 24:54,54
	# JHN 21:26,26
	def convertChapterStart(self, bookId):
		if bookId == "MAT":
			return (28, "21", "21")
		elif bookId == "MRK":
			return (16, "21", "21")
		elif bookId == "LUK":
			return (24, "54", "54")
		elif bookId == "JHN":
			return (21, "26", "26")
		else:
			print("ERROR: Unexpected book %s in UpdateDBPDatabase." % (bookId))
			sys.exit()


	def getDuration(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
		result = self.durationRegex.search(str(response))
		if result != None:
			dur = result.group(1)
			#print("duration", dur)
			return int(math.ceil(float(dur)))
		else:
			raise Exception("ffprobe for duration failed for %s = %s" % (file, response.stderr))


## Unit Test
if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator	
	from InputProcessor import InputProcessor
	from AWSSession import AWSSession
	from DBPLoadController import DBPLoadController

	config = Config.shared()
	languageReader = LanguageReaderCreator("BLIMP").create("")
	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, languageReader)
	ctrl.validate(filesets)

	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut, languageReader)
	for inp in InputFileset.upload:
		hashId = update.processFileset(inp)

	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-" + inp.filesetId)

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test UNRWFWP1DA
# time python3 load/TestCleanup.py test UNRWFWP1DA16
# time python3 load/UpdateDBPFilesetTables.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA
# time python3 load/UpdateDBPFilesetTables.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA16
# time python3 load/UpdateDBPFilesetTables.py

# python3  load/UpdateDBPFilesetTables.py test s3://etl-development-input Spanish_N2SPNTLA_USX
# python3  load/UpdateDBPFilesetTables.py test s3://etl-development-input SPNBDAP2DV
# python3  load/UpdateDBPFilesetTables.py test s3://etl-development-input ENGESVN2DA
