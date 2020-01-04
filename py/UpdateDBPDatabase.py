# UpdateDBPDatabase.py
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
from Config import *
from SQLUtility import *


class UpdateDBPDatabase:

	def __init__(self, config):
		print("init")
		self.config = config
		self.db = SQLUtility(config.database_host, config.database_port, config.database_user, config.database_db_name)
		self.statements = []


	def process(self):
		print("process")
		dirname = self.config.directory_accepted
		files = os.listdir(dirname)
		for file in sorted(files):
			if file.endswith("csv"):
				(bucket, typeCode, bibleId, filesetId) = file.split(".")[0].split("_")
				print(bucket, typeCode, bibleId, filesetId)
				setTypeCode = self.getSetTypeCode(typeCode, filesetId)
				hashId = self.getHashId(bucket, filesetId, setTypeCode)
				csvFilename = dirname + os.sep + file
				setSizeCode = self.getSetSizeCode(csvFilename)
				self.statements = []
				self.deleteBibleFiles(hashId)
				self.insertBibleFileset(bucket, filesetId, hashId, setTypeCode, setSizeCode)
				self.insertBibleFiles(hashId, csvFilename)
				self.db.executeTransaction(self.statements)


	def getSetTypeCode(self, typeCode, filesetId):
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


	def getHashId(self, bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def getSetSizeCode(self, csvFilename):
		return "C"
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])

		## somehow determine the size based upon the number of books?
		# match to old testament set, and new testament set
		#| C             |      441 |
		#| NT            |     5535 |
		#| OT            |       95 |
		#| NTOTP         |      268 |
		#| OTNTP         |        2 |
		#| NTPOTP        |      115 |
		#| NTP           |      841 |
		#| OTP           |      177 |
		#| P             |       13 |
		#| S             |       24 |


	def insertBibleFileset(self, bucket, filesetId, hashId, setTypeCode, setSizeCode):
		sql = ("REPLACE INTO bible_filesets(id, hash_id, asset_id, set_type_code,"
			" set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, 0)")
		values = (filesetId, hashId, bucket, setTypeCode, setSizeCode)
		self.statements.append((sql, [values]))


	def deleteBibleFiles(self, hashId):
		sql = "DELETE FROM bible_files WHERE hash_id = %s"
		self.statements.append((sql, [(hashId,)]))


	def insertBibleFiles(self, hashId, csvFilename):
		#typeCode,bible_id,fileset_id,file_name,book_id,chapter_start,chapter_end,verse_start,verse_end,errors
		## skipped duration and file_size
		sql = ("INSERT INTO bible_files(hash_id, book_id, chapter_start, chapter_end,"
			" verse_start, verse_end, file_name) VALUES (%s, %s, %s, %s, %s, %s, %s)")
		values = []
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookId = row["book_id"] if row["book_id"] != "" else None
				if row["chapter_start"] == "end":
					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
				else:
					chapterStart = row["chapter_start"] if row["chapter_start"] != "" else "1"
					verseStart = row["verse_start"] if row["verse_start"] != "" else "1"
					verseEnd = row["verse_end"] if row["verse_end"] != "" else None
				chapterEnd = row["chapter_end"] if row["chapter_end"] != "" else None
				value = (hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, row["file_name"])
				values.append(value)
			self.statements.append((sql, values))


	def getValue(row, name):
		return row[name] if row[name] != "" else None
		if row[name] != "":
			return row[name]



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
			return ("1", "1", "1")



config = Config("dev")
update = UpdateDBPDatabase(config)
update.process()




