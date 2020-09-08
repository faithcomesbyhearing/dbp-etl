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
import re
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from LPTSExtractReader import *
#from LookupTables import *


class UpdateDBPBiblesTable:

	def getHashId(bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def getSetTypeCode(typeCode, filesetId):
		if typeCode == "text":
			return "text_format"
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
		self.numeralIdMap = self.db.selectMap("SELECT script_id, numeral_system_id FROM alphabet_numeral_systems", ())



	def processOld(self, lptsReader):
		print("process")
		dirname = self.config.directory_accepted
		files = os.listdir(dirname)
		for file in sorted(files):
			if file.endswith("csv"):
				(typeCode, bibleId, filesetId) = file.split(".")[0].split("_")
				bucket = self.config.s3_vid_bucket if typeCode == "video" else self.config.s3_bucket
				print(bucket, typeCode, bibleId, filesetId)
				(lptsRecord, lptsIndex, damIdStatus) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				print("LPTS", lptsRecord.Reg_StockNumber(), lptsIndex, damIdStatus)
				setTypeCode = UpdateDBPDatabase.getSetTypeCode(typeCode, filesetId)
				hashId = UpdateDBPDatabase.getHashId(bucket, filesetId, setTypeCode)
				csvFilename = dirname + os.sep + file
				setSizeCode = self.getSetSizeCodeByFile(csvFilename)
				self.statements = []
				#self.rejectStatements = []
				self.deleteBibleFiles(hashId)
				self.insertBibleFileset(bucket, filesetId, hashId, setTypeCode, setSizeCode)
				self.insertBibleFiles(hashId, csvFilename)
				self.insertBibles(bibleId, lptsRecord, lptsIndex, setSizeCode)
				self.insertBibleFilesetConnections(bibleId, hashId)
				self.insertBibleTranslations()
				self.insertBibleEquivalents() #last updated 2018-11-21 17:07:08 
				self.insertBibleFileTitles() #last updated 2018-03-05 03:32:33
				self.insertBibleLinks() #last updated 2019-04-26 15:48:17
				self.insertBibleOrganizations() # last updated 2019-04-30 10:49:05
				self.db.displayTransaction(self.statements)
				#if len(self.rejectStatements) == 0:
				self.db.executeTransaction(self.statements)


		if len(self.rejectStatements) > 0:
			print("\n\nREJECT\n\n")
			self.db.displayTransaction(self.rejectStatements)


	def process(self, lptsReader):
		dirname = self.config.directory_database
		typeCodeList = os.listdir(dirname)
		for typeCode in typeCodeList:
			bibleIdPath = dirname + os.sep + typeCode
			if typeCode == "audio":
				for bibleId in os.listdir(bibleIdPath):
					filesetIdPath = bibleIdPath + os.sep + bibleId
					for filesetId in os.listdir(filesetIdPath):
						print(typeCode, bibleId, filesetId)
						csvFilename = "%s/%s_%s_%s.csv" % (self.config.directory_accepted, typeCode, bibleId, filesetId)
						hashId = self.insertBibleFileset(typeCode, filesetId, csvFilename)
						self.insertFilesetConnections(hashId, bibleId)
						filePath = filesetIdPath + os.sep + filesetId
						for filename in os.listdir(filePath):
							print(filename)
						# I should read the csv file, not the filelist, because it contains
						# the required information
			elif typeCode == "text":
				print("TBD text update")
			elif typeCode == "video":
				print("TBD video update")



	def getSetSizeCodeByFile(self, csvFilename):
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])
		otBooks = bookIdSet.intersection(self.OT)
		ntBooks = bookIdSet.intersection(self.NT)
		return UpdateDBPDatabase.getSetSizeCode(ntBooks, otBooks)


#	def deleteBibleFiles(self, hashId):
#		sql = []
#		sql.append("DELETE bfss FROM bible_file_stream_ts AS bfss"
#			" JOIN bible_file_stream_bandwidths AS bfsb ON bfss.stream_bandwidth_id = bfsb.id"
#			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
#			" WHERE bf.hash_id = %s")
#		sql.append("DELETE bfsb FROM bible_file_stream_bandwidths AS bfsb"
#			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
#			" WHERE bf.hash_id = %s")
#		sql.append("DELETE bft FROM bible_file_tags AS bft"
#			" JOIN bible_files AS bf ON bft.file_id = bf.id"
#			" WHERE bf.hash_id = %s")
#		sql.append("DELETE FROM bible_files WHERE hash_id = %s")
#		sql.append("DELETE FROM access_group_filesets WHERE hash_id = %s")
#		sql.append("DELETE FROM bible_fileset_connections WHERE hash_id = %s")
#		sql.append("DELETE FROM bible_fileset_tags WHERE hash_id = %s")
#		sql.append("DELETE FROM bible_filesets WHERE hash_id = %s")
#		for stmt in sql:
#			self.statements.append((stmt, [(hashId,)]))


	def insertBibleFileset(self, typeCode, filesetId, csvFilename):
		bucket = self.config.s3_vid_bucket if typeCode == "video" else self.config.s3_bucket
		setTypeCode = UpdateDBPDatabase.getSetTypeCode(typeCode, filesetId)
		setSizeCode = self.getSetSizeCodeByFile(csvFilename)
		hashId = UpdateDBPDatabase.getHashId(bucket, filesetId, setTypeCode)
		insertRows = []
		insertRows.append((filesetId, bucket, setTypeCode, setSizeCode, hashId))
		tableName = "bible_filesets"
		pkeyNames = ("hash_id",)
		attrNames = ("id", "asset_id", "set_type_code", "set_size_code")
		self.dbOut.replace(tableName, pkeyNames, attrNames, insertRows)
		return hashId


	def insertFilesetConnections(self, hashId, bibleId):
		insertRows = []
		insertRows.append((hashId, bibleId))
		tableName = "bible_fileset_connections"
		pkeyNames = ("hash_id", "bible_id")
		attrNames = ()
		self.dbOut.replace(tableName, pkeyNames, attrNames, insertRows)


	def insertBibleFiles(self, hashId, csvFilename):
		insertRows = []
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				#sequence = row["sequence"]
				bookId = row["book_id"]
				chapterStart = row["chapter_start"]
				chapterEnd = row["chapter_end"]
				verseStart = row["verse_start"]
				verseEnd = row["verse_end"]
				fileName = row["file_name"]
				fileSize = row["file_size"]
				duration = 0
				if chapterStart == "end":
					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
				insertRows.append((chapterEnd, verseEnd, fileName, fileSize, duration,
					hashId, bookId, chapterStart, chapterEnd))
		tableName = "bible_files"
		pkeyNames = ("hash_id", "book_id", "chapter_start", "verse_start")
		attrNames = ("chapter_end", "verse_end", "file_name", "file_size", "duration")
		self.dbOut.replace(tableName, pkeyNames, attrNames, insertRows)

#		isVideoFile = csvFilename.split("_")[1] == "video"
#		## skipped duration
#		sql = ("INSERT INTO bible_files(hash_id, book_id, chapter_start, chapter_end,"
#			" verse_start, verse_end, file_name, file_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
#		values = []
#		with open(csvFilename, newline='\n') as csvfile:
#			reader = csv.DictReader(csvfile)
#			for row in reader:
#				bookId = row["book_id"] if row["book_id"] != "" else None
#				if row["chapter_start"] == "end":
#					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
#				else:
#					chapterStart = row["chapter_start"] if row["chapter_start"] != "" else None
#					verseStart = row["verse_start"] if row["verse_start"] != "" else None
#					verseEnd = row["verse_end"] if row["verse_end"] != "" else None
#				chapterEnd = row["chapter_end"] if row["chapter_end"] != "" else None
#				if isVideoFile:
#					filename = row["file_name"].split(".")[0] + "_stream.m3u8"
#					fullname = "%s/%s/%s/%s" % (row["type_code"], row["bible_id"], row["fileset_id"], filename)
#					if fullname in self.videoStreamSet:
#						value = (hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, filename, row["file_size"])
#						values.append(value)
#					else:
#						print("ERROR: %s is missing" % (fullname))
#				else:
#					value = (hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, row["file_name"], row["file_size"])
#					values.append(value)
#			self.statements.append((sql, values))


	def convertChapterStart(self, bookId):
		if bookId == "MAT":
			return ("28", "21", "21")
		elif bookId == "MRK":
			return ("16", "21", "21")
		elif bookId == "LUK":
			return ("24", "54", "54")
		elif bookId == "JHN":
			return ("21", "26", "26")
		else:
			print("ERROR: Unexpected book %s in UpdateDBPDatabase." % (bookId))
			sys.exit()	


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	update = UpdateDBPBiblesTable(config, db, dbOut)
	update.process(lptsReader)

	dbOut.displayCounts()
	dbOut.displayStatements()
	#dbOut.execute()


