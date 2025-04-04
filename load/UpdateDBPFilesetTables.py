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

			if inp.typeCode == "video":
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



	def insertBibleFiles(self, dbConn, hashId, inputFileset, bookIdSet):
		inp = inputFileset
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = ("SELECT book_id, chapter_start, verse_start, chapter_end, verse_end, file_name, file_size, duration"
				" FROM bible_files WHERE hash_id = %s AND book_id IN ('") + "','".join(bookIdSet) + "')"
		resultSet = dbConn.select(sql, (hashId,))
		dbpMap = {}
		for row in resultSet:
			(dbpBookId, dbpChapterStart, dbpVerseStart, dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration) = row
			key = (dbpBookId, dbpChapterStart, dbpVerseStart)
			value = (dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration)
			dbpMap[key] = value
		with open(inp.csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookId = row["book_id"]
				if row["chapter_start"] == "end":
					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
				else:
					chapterStart = int(row["chapter_start"]) if row["chapter_start"] != "" else None
					verseStart = row["verse_start"] if row["verse_start"] != "" else 1
					verseSequence = int(row["verse_sequence"]) if row["verse_sequence"] != 0 or row["verse_sequence"] != "" else 1
					verseEnd = row["verse_end"] if row["verse_end"] != "" else None

				chapterEnd = int(row["chapter_end"]) if row["chapter_end"] != "" else None
				fileName = row["file_name"]
				if inp.typeCode == "video":
					fileName = fileName.split(".")[0] + "_stream.m3u8"
				fileSize = int(row["file_size"]) if row["file_size"] != "" else None
				inputFile = inp.getInputFile(fileName)
				duration = inputFile.duration if inputFile != None else None
				if inp.typeCode == "audio" and inp.locationType == InputFileset.LOCAL:
					duration = self.getDuration(inp.fullPath() + os.sep + fileName)
				key = (bookId, chapterStart, verseStart)
				dbpValue = dbpMap.get(key)
				if dbpValue == None:
					insertRows.append((chapterEnd, verseEnd, fileName, fileSize, duration, verseSequence,
						hashId, bookId, chapterStart, verseStart))
				else:
					del dbpMap[key]
					(dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration) = dbpValue

					# If the duration value is empty, it will keep the value retrieved from the database.
					if inp.typeCode == "video" and duration == None and (dbpDuration != None or dbpDuration != ""):
						duration = dbpDuration

					if (chapterEnd != dbpChapterEnd or
						verseEnd != dbpVerseEnd or
						fileName != dbpFileName or
						fileSize != dbpFileSize or
						(duration != dbpDuration and duration != None and duration != "") ):
						updateRows.append((chapterEnd, verseEnd, fileName, fileSize, duration, verseSequence,
						hashId, bookId, chapterStart, verseStart))

		if inp.typeCode == "video":
			for (dbpBookId, dbpChapterStart, dbpVerseStart) in dbpMap.keys():
				deleteRows.append((hashId, dbpBookId, dbpChapterStart, dbpVerseStart))

		tableName = "bible_files"
		pkeyNames = ("hash_id", "book_id", "chapter_start", "verse_start")
		attrNames = ("chapter_end", "verse_end", "file_name", "file_size", "duration", "verse_sequence")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows, 2)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


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
