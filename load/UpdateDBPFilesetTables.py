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
from SQLBatchExec import *


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
		self.durationRegex = re.compile(r"duration=([0-9\.]+)")


	def process(self):
		results = []
		dirname = self.config.directory_accepted
		filenameList = os.listdir(dirname)
		for filename in filenameList:
			if filename.endswith(".csv"):
				(typeCode, bibleId, filesetId) = filename.split(".")[0].split("_")
				print(typeCode, bibleId, filesetId)
				csvFilename = self.config.directory_accepted + filename
				if typeCode == "audio":
					hashId = self.insertBibleFileset(typeCode, filesetId, csvFilename)
					self.insertFilesetConnections(hashId, bibleId)
					filesetDir = "%s%s/%s/%s" % (self.config.directory_database, typeCode, bibleId, filesetId)
					self.insertBibleFiles(hashId, csvFilename, filesetDir)
					results.append("%s/%s/%s" % (typeCode, bibleId, filesetId))

				elif typeCode == "text":
					print("TBD text update")
					sys.exit()
				elif typeCode == "video":
					print("TBD video update")
					sys.exit()
		return results


	def getSetSizeCodeByFile(self, csvFilename):
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])
		otBooks = bookIdSet.intersection(self.OT)
		ntBooks = bookIdSet.intersection(self.NT)
		return UpdateDBPFilesetTables.getSetSizeCode(ntBooks, otBooks)


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
		setTypeCode = UpdateDBPFilesetTables.getSetTypeCode(typeCode, filesetId)
		setSizeCode = self.getSetSizeCodeByFile(csvFilename)
		hashId = UpdateDBPFilesetTables.getHashId(bucket, filesetId, setTypeCode)
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


	def insertBibleFiles(self, hashId, csvFilename, filesetDir):
		insertRows = []
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookId = row["book_id"]
				if row["chapter_start"] == "end":
					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
				else:
					chapterStart = row["chapter_start"]
					verseStart = row["verse_start"] if row["verse_start"] != "" else "1"
					verseEnd = row["verse_end"] if row["verse_end"] != "" else None
				chapterEnd = row["chapter_end"] if row["chapter_end"] != "" else None
				fileName = row["file_name"]
				fileSize = row["file_size"]
				duration = self.getDuration(filesetDir + os.sep + fileName)
				insertRows.append((chapterEnd, verseEnd, fileName, fileSize, duration,
					hashId, bookId, chapterStart, verseStart))
		tableName = "bible_files"
		pkeyNames = ("hash_id", "book_id", "chapter_start", "verse_start")
		attrNames = ("chapter_end", "verse_end", "file_name", "file_size", "duration")
		self.dbOut.replace(tableName, pkeyNames, attrNames, insertRows)


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


	def getDuration(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep duration'
		response = subprocess.run(cmd, shell=True, capture_output=True)
		result = self.durationRegex.search(str(response))
		if result != None:
			dur = result.group(1)
			#print("duration", dur)
			return int(math.ceil(float(dur)))
		else:
			raise Exception("ffprobe for duration failed for %s" % (file))


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut)
	update.process()

	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute()


##
## delete fileset_id, requires a check in dbp_users.playlist_items (fileset_id)
##


