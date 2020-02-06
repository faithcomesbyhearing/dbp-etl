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
from LPTSExtractReader import *
from LookupTables import *


class UpdateDBPDatabase:

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


	def __init__(self, config):
		self.config = config
		self.db = SQLUtility(config)
		self.statements = []
		self.rejectStatements = []
		self.OT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'OT'", ())
		self.NT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'NT'", ())
		self.numeralIdMap = self.db.selectMap("SELECT script_id, numeral_system_id FROM alphabet_numeral_systems", ())
		self.videoStreamSet = set()
		bucketPath = config.directory_bucket_list + os.sep + config.s3_vid_bucket + ".txt"
		fp = open(bucketPath, "r")
		for line in fp:
			file = line.split("\t")[0]
			filename = file.split("/")[-1]
			if filename.endswith("_stream.m3u8"):
				self.videoStreamSet.add(file)
		fp.close()


	def process(self, lptsReader):
		print("process")
		dirname = self.config.directory_accepted
		files = os.listdir(dirname)
		#for file in sorted(files):
		for file in sorted(files[:10]): # do 10
			if file.endswith("csv"):
				(typeCode, bibleId, filesetId) = file.split(".")[0].split("_")
				bucket = self.config.s3_vid_bucket if typeCode == "video" else self.config.s3_bucket
				print(bucket, typeCode, bibleId, filesetId)
				(lptsRecord, lptsIndex, damIdStatus) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				print("LPTS", lptsRecord, lptsIndex, damIdStatus)
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
				#if len(rejectStatements) == 0:
					#self.db.executeTransaction(self.statements)
				self.db.displayTransaction(self.statements)

		if len(self.rejectStatements) > 0:
			print("\n\nREJECT\n\n")
			self.db.displayTransaction(self.rejectStatements)


	def getSetSizeCodeByFile(self, csvFilename):
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])
		otBooks = bookIdSet.intersection(self.OT)
		ntBooks = bookIdSet.intersection(self.NT)
		return UpdateDBPDatabase.getSetSizeCode(ntBooks, otBooks)


	def insertBibleFileset(self, bucket, filesetId, hashId, setTypeCode, setSizeCode):
		sql = ("INSERT INTO bible_filesets(id, hash_id, asset_id, set_type_code,"
			" set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, 0)")
		values = (filesetId, hashId, bucket, setTypeCode, setSizeCode)
		self.statements.append((sql, [values]))


	def deleteBibleFiles(self, hashId):
		sql = "DELETE FROM bible_files WHERE hash_id = %s"
		self.statements.append((sql, [(hashId,)]))
		sql = "DELETE FROM bible_fileset_connections WHERE hash_id = %s"
		self.statements.append((sql, [(hashId,)]))
		sql = "DELETE FROM bible_filesets WHERE hash_id = %s"
		self.statements.append((sql, [(hashId,)]))


	def insertBibleFiles(self, hashId, csvFilename):
		isVideoFile = csvFilename.split("_")[1] == "video"
		## skipped duration
		sql = ("INSERT INTO bible_files(hash_id, book_id, chapter_start, chapter_end,"
			" verse_start, verse_end, file_name, file_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
		values = []
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookId = row["book_id"] if row["book_id"] != "" else None
				if row["chapter_start"] == "end":
					(chapterStart, verseStart, verseEnd) = self.convertChapterStart(bookId)
				else:
					chapterStart = row["chapter_start"] if row["chapter_start"] != "" else None
					verseStart = row["verse_start"] if row["verse_start"] != "" else None
					verseEnd = row["verse_end"] if row["verse_end"] != "" else None
				chapterEnd = row["chapter_end"] if row["chapter_end"] != "" else None
				if isVideoFile:
					filename = row["file_name"].split(".")[0] + "_stream.m3u8"
					fullname = "%s/%s/%s/%s" % (row["type_code"], row["bible_id"], row["fileset_id"], filename)
					if fullname in self.videoStreamSet:
						value = (hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, filename, row["file_size"])
						values.append(value)
					else:
						print("ERROR: %s is missing" % (fullname))
				else:
					value = (hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, row["file_name"], row["file_size"])
					values.append(value)
			self.statements.append((sql, values))


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


	def insertBibles(self, bibleId, lptsRecord, lptsIndex, setSizeCode):
		languageId = self.bibles_languageId(bibleId, lptsRecord)
		versification = ""#"protestant" ## need input from Alan
		script = self.bibles_script(bibleId, lptsRecord, lptsIndex)
		## Bug: Arab and Deva have 2 numeral systems and I have no way to choose
		numeralSystemId = self.numeralIdMap.get(script) 
		date = self.bibles_date(lptsRecord)
		scope = setSizeCode
		derived = None
		copyright = self.bibles_copyright(lptsRecord)
		priority = self.bibles_priority(bibleId)
		reviewed = 0#self.bibles_reviewed(lptsRecord)
		notes = None
		## This is only populating bibleId. It will need to update when it already exists
		#sql = "SELECT id FROM bibles WHERE id = %s"
		#result = self.db.selectRow(sql, (bibleId,))
		#if result == None:
		sql = ("INSERT INTO bibles (id, language_id, versification, numeral_system_id, `date`,"
				" scope, script, derived, copyright, priority, reviewed, notes) VALUES"
				" (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
		values = [(bibleId, languageId, versification, numeralSystemId, date,
				scope, script, derived, copyright, priority, reviewed, notes)]
		if languageId == None or script == None or numeralSystemId == None:
			self.rejectStatements.append((sql, values))
		else:
			self.statements.append((sql, values))
		## else: check if new column values are different and update or replace if they are


	def bibles_languageId(self, bibleId, lptsRecord):
		result = None
		if lptsRecord != None:
			iso = lptsRecord.ISO()
			langName = lptsRecord.LangName()
			result = self.db.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			if result != None:
				return result
			result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
			if result != None:
				return result
		else:
			iso = bibleId[:3].lower()
		result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
		return result


	def bibles_script(self, bibleId, lptsRecord, lptsIndex):
		result = None
		script = lptsRecord.Orthography(lptsIndex)
		if script != None:
			result = LookupTables.scriptCode(script)
		return result


	def bibles_date(self, lptsRecord):
		volume = lptsRecord.Volumne_Name()
		yearPattern = re.compile("([0-9]+)")
		match = yearPattern.search(volume)
		if match != None:
			return match.group(1)
		else:
			return None


	def bibles_copyright(self, lptsRecord):
		if "Wycliffe" in lptsRecord.Copyrightc() and "Hosana" in lptsRecord.Copyrightp():
			return "BY-NC-ND"
		else:
			return "'" + lptsRecord.Copyrightc() + "'"


	def bibles_priority(self, bibleId):
		priority = {"ENGESV": 20,
					"ENGNIV": 19,
					"ENGKJV": 18,
					"ENGCEV": 17,
					"ENGNRSV": 16,
					"ENGNAB": 15,
					"ENGWEB": 14,
					"ENGNAS": 13,
					"ENGNLV": 11,
					"ENGEVD": 10}
		return priority.get(bibleId, 0)


	def insertBibleFilesetConnections(self, bibleId, hashId):
		sql = "INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)"
		self.statements.append((sql, [(hashId, bibleId)]))


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	update = UpdateDBPDatabase(config)
	update.process(lptsReader)


