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

import io
import sys
import hashlib
import csv
import math
import subprocess
from Config import *
from SQLUtility import *
from SqliteUtility import *
from SQLBatchExec import *
from UpdateDBPTextFilesets import *
from UpdateDBPBooksTable import *


class UpdateDBPFilesetTables:

	def getHashId(bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def getSetTypeCode(typeCode, filesetId):
		if typeCode == "text":
			return "text_plain"
		elif typeCode == "video":
			return "video_stream"
		elif typeCode == "audio":
			code = filesetId[7:9]
			if code == "1D":
				return "audio"
			elif code == "2D":
				return "audio_drama"
			else:
				code = filesetId[8:10]
				if code == "1D":
					return "audio"
				elif code == "2D":
					return "audio_drama"
				elif filesetId == "N1TUVDPI":
					return "audio"
				elif filesetId == "O1TUVDPI":
					return "audio"
				else:
					print("ERROR: file type not known for %s, set_type_code set to 'unknown'" % (filesetId))
					#return "unknown"
					sys.exit()
		else:
			print("ERROR: type code %s is not know for %s" % (typeCode, filesetId))
			sys.exit()


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


	def __init__(self, config, db, dbOut):
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


	def processFileset(self, inputFileset):
		inp = inputFileset
		print(inp.typeCode, inp.bibleId, inp.filesetId)
		bookIdSet = self.getBibleBooks(inp.csvFilename)
		hashId = self.insertBibleFileset(inp.typeCode, inp.bibleId, inp.filesetId, bookIdSet)
		self.insertFilesetConnections(hashId, inp.bibleId)
		if inp.typeCode in {"audio", "video"}:
			self.insertBibleFiles(hashId, inputFileset, bookIdSet)
		elif inp.typeCode == "text":
			self.textUpdater.updateFileset(inp.bibleId, inp.filesetId, hashId, bookIdSet, inp.databasePath)

		tocBooks = self.booksUpdater.getTableOfContents(inp.typeCode, inp.bibleId, inp.filesetId, inp.csvFilename, inp.databasePath)
		self.booksUpdater.updateBibleBooks(inp.typeCode, inp.bibleId, tocBooks)
		return hashId


	def getBibleBooks(self, csvFilename):
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])
		return bookIdSet


	def getSizeCode(self, typeCode, hashId, bookIdSet):
		if typeCode == "text":
			existingBookIdSet = self.db.selectSet("SELECT book_id FROM bible_verses WHERE hash_id = %s", (hashId,))
		else:
			existingBookIdSet = self.db.selectSet("SELECT book_id FROM bible_files WHERE hash_id = %s", (hashId,))			
		fullBookIdSet = existingBookIdSet.union(bookIdSet)
		otBooks = fullBookIdSet.intersection(self.OT)
		ntBooks = fullBookIdSet.intersection(self.NT)
		return UpdateDBPFilesetTables.getSetSizeCode(ntBooks, otBooks)


	def insertBibleFileset(self, typeCode, bibleId, filesetId, bookIdSet):
		tableName = "bible_filesets"
		pkeyNames = ("hash_id",)
		attrNames = ("id", "asset_id", "set_type_code", "set_size_code")
		updateRows = []
		bucket = self.config.s3_vid_bucket if typeCode == "video" else self.config.s3_bucket
		setTypeCode = UpdateDBPFilesetTables.getSetTypeCode(typeCode, filesetId)
		hashId = UpdateDBPFilesetTables.getHashId(bucket, filesetId, setTypeCode)
		setSizeCode = self.getSizeCode(typeCode, hashId, bookIdSet)
		row = self.db.selectRow("SELECT id, asset_id, set_type_code, set_size_code FROM bible_filesets WHERE hash_id=%s", (hashId,))
		if row == None:
			updateRows.append((filesetId, bucket, setTypeCode, setSizeCode, hashId))
			self.dbOut.insert(tableName, pkeyNames, attrNames, updateRows)
		else:
			(dbpFilesetId, dbpBucket, dbpSetTypeCode, dbpSetSizeCode) = row
			if (dbpFilesetId != filesetId or
				dbpBucket != bucket or
				dbpSetTypeCode != setTypeCode or
				dbpSetSizeCode != setSizeCode):
				updateRows.append((filesetId, bucket, setTypeCode, setSizeCode, hashId))
				self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		return hashId


	def insertFilesetConnections(self, hashId, bibleId):
		insertRows = []
		row = self.db.selectRow("SELECT * FROM bible_fileset_connections WHERE hash_id=%s AND bible_id=%s", (hashId, bibleId))
		if row == None:
			insertRows.append((hashId, bibleId))
			tableName = "bible_fileset_connections"
			pkeyNames = ("hash_id", "bible_id")
			attrNames = ()
			self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


	def insertBibleFiles(self, hashId, inputFileset, bookIdSet):
		inp = inputFileset
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = ("SELECT book_id, chapter_start, verse_start, chapter_end, verse_end, file_name, file_size, duration"
				" FROM bible_files WHERE hash_id = %s AND book_id IN ('") + "','".join(bookIdSet) + "')"
		resultSet = self.db.select(sql, (hashId,))
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
					verseStart = int(row["verse_start"]) if row["verse_start"] != "" else 1
					verseEnd = int(row["verse_end"]) if row["verse_end"] != "" else None
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
					insertRows.append((chapterEnd, verseEnd, fileName, fileSize, duration,
						hashId, bookId, chapterStart, verseStart))
				else:
					del dbpMap[key]
					(dbpChapterEnd, dbpVerseEnd, dbpFileName, dbpFileSize, dbpDuration) = dbpValue
					if (chapterEnd != dbpChapterEnd or
						verseEnd != dbpVerseEnd or
						fileName != dbpFileName or
						fileSize != dbpFileSize or
						duration != dbpDuration):
						updateRows.append((chapterEnd, verseEnd, fileName, fileSize, duration,
						hashId, bookId, chapterStart, verseStart))

		if inp.typeCode == "video":
			for (dbpBookId, dbpChapterStart, dbpVerseStart) in dbpMap.keys():
				deleteRows.append((hashId, dbpBookId, dbpChapterStart, dbpVerseStart))

		tableName = "bible_files"
		pkeyNames = ("hash_id", "book_id", "chapter_start", "verse_start")
		attrNames = ("chapter_end", "verse_end", "file_name", "file_size", "duration")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows, 2)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def convertChapterStart(self, bookId):
		if bookId == "MAT":
			return (28, 21, 21)
		elif bookId == "MRK":
			return (16, 21, 21)
		elif bookId == "LUK":
			return (24, 54, 54)
		elif bookId == "JHN":
			return (21, 26, 26)
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
	from LPTSExtractReader import *
	from InputFileset import *
	from DBPLoadController import *

	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.validate(filesets)

	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut)
	for inp in InputFileset.upload:
		hashId = update.processFileset(inp.typeCode, inp.bibleId, inp.filesetId, inp.fullPath(), inp.csvFilename, inp.databasePath)

	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-" + inp.filesetId)

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test UNRWFWP1DA
# time python3 load/TestCleanup.py test UNRWFWP1DA16
# time python3 load/UpdateDBPFilesetTables.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA
# time python3 load/UpdateDBPFilesetTables.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA16


