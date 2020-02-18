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
import re
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
				self.insertBibleFilesetTags(typeCode, filesetId, hashId, lptsRecord)
				#self.insertBibleFilesetCopyrights(typeCode, hashId, lptsRecord)
				self.insertBibleFilesetCopyrightOrganizations()
				self.insertAccessGroupFilesets()
				self.insertBibleFiles(hashId, csvFilename)
				##self.insertBibleFileTags(hashId) Develop if needed
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


	def getSetSizeCodeByFile(self, csvFilename):
		bookIdSet = set()
		with open(csvFilename, newline='\n') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				bookIdSet.add(row["book_id"])
		otBooks = bookIdSet.intersection(self.OT)
		ntBooks = bookIdSet.intersection(self.NT)
		return UpdateDBPDatabase.getSetSizeCode(ntBooks, otBooks)


	def deleteBibleFiles(self, hashId):
		sql = []
		sql.append("DELETE bfss FROM bible_file_stream_ts AS bfss"
			" JOIN bible_file_stream_bandwidths AS bfsb ON bfss.stream_bandwidth_id = bfsb.id"
			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE bfsb FROM bible_file_stream_bandwidths AS bfsb"
			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE bft FROM bible_file_tags AS bft"
			" JOIN bible_files AS bf ON bft.file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE FROM bible_files WHERE hash_id = %s")
		sql.append("DELETE FROM access_group_filesets WHERE hash_id = %s")
		sql.append("DELETE FROM bible_fileset_connections WHERE hash_id = %s")
		sql.append("DELETE FROM bible_fileset_tags WHERE hash_id = %s")
		sql.append("DELETE FROM bible_filesets WHERE hash_id = %s")
		for stmt in sql:
			self.statements.append((stmt, [(hashId,)]))


	def insertBibleFileset(self, bucket, filesetId, hashId, setTypeCode, setSizeCode):
		sql = ("INSERT INTO bible_filesets(id, hash_id, asset_id, set_type_code,"
			" set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, 0)")
		values = (filesetId, hashId, bucket, setTypeCode, setSizeCode)
		self.statements.append((sql, [values]))


	def insertBibleFilesetTags(self, typeCode, filesetId, hashId, lptsRecord):
		sql = ("INSERT INTO bible_fileset_tags(hash_id, name, description, admin_only,"
			" notes, iso, language_id) VALUES (%s, %s, %s, 0, NULL, 'eng', 6414)")
		values = []
		if typeCode == "audio":
			bitrate = filesetId[10:]
			if bitrate == "":
				bitrate = "64"
			values.append((hashId, 'bitrate', bitrate + "kbs"))
		stockNo = lptsRecord.Reg_StockNumber()
		if stockNo != None:
			values.append((hashId, 'sku', stockNo.replace("/", "")))
		volume = lptsRecord.Volumne_Name()
		if volume != None:
			values.append((hashId, 'volume', volume))
		self.statements.append((sql, values))


#	def insertBibleFilesetCopyrights(self, typeCode, hashId, lptsRecord):
#		## primary key is hash_id
#		sql = ("INSERT INTO bible_fileset_copyrights(hash_id, copyright_date,"
#			" copyright, copyright_description) VALUES (%s, %s, %s, %s)")
#		copyrightText = lptsRecord.Copyrightc()
#		copyrightAudio = lptsRecord.Copyrightp()
#		copyrightVideo = lptsRecord.Copyright_Video()
#
#		if typeCode == "text":
#			copyright = copyrightText
#			copyrightMsg = copyrightText
#		elif typeCode == "audio":
#			copyright = copyrightAudio
#			copyrightMsg = "Text: %s\nAudio: %s" % (copyrightText, copyrightAudio)
#		elif typeCode == "video":
#			copyright = copyrightVideo
#			copyrightMsg = "Text: %s\nAudio: %s\nVideo: %s" % (copyrightText, copyrightAudio, copyrightVideo)
#
#		copyrightDate = None
#		if copyright != None:
#			datePattern = re.compile("([0-9]+)")
#			year = datePattern.search(copyright)
#			if year != None:
#				copyrightDate = year.group(1)
#				## Should I work on finding multiple dates?
#		values = (hashId, copyrightDate, copyrightMsg, "")
#		self.statements.append((sql, [values]))


	def insertBibleFilesetCopyrightOrganizations(self):
		## primary key is hash_id, organization_id
		sql = ("INSERT INTO bible_fileset_copyright_organizations(hash_id,"
			" organization_id, organization_role) VALUES (%s, %s, %s)")
		# Not implemented


	def insertAccessGroupFilesets(self):
		## primary key is access_group_id, hash_id
		sql = ("INSERT INTO access_group_filesets(access_group_id, hash_id) VALUES (%s, %s)")
		# Not implemented


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
		## primary key is id (bibleId)
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
		sql = "SELECT id FROM bibles WHERE id = %s"
		result = self.db.selectRow(sql, (bibleId,))
		if result == None:
			sql = ("INSERT INTO bibles (language_id, versification, numeral_system_id, `date`,"
				" scope, script, derived, copyright, priority, reviewed, notes, id) VALUES"
				" (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
		else:
			sql = ("UPDATE bibles SET language_id = %s, versification = %s, numeral_system_id = %s,"
				" `date` = %s, scope = %s, script = %s, derived = %s, copyright = %s, priority = %s,"
				" reviewed = %s, notes = %s WHERE id = %s")
		values = [(languageId, versification, numeralSystemId, date,
			scope, script, derived, copyright, priority, reviewed, notes, bibleId)]
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
		if volume != None:
			yearPattern = re.compile("([0-9]+)")
			match = yearPattern.search(volume)
			if match != None:
				return match.group(1)
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
		## primary key hash_id, bible_id
		sql = "INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)"
		self.statements.append((sql, [(hashId, bibleId)]))


	def insertBibleTranslations(self):
		## primary key id is auto increment
		sql = ("INSERT INTO bible_translations(language_id, bible_id, vernacular, vernacular_trade,"
			" name, description, background, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
		# Not implemented


	def insertBibleEquivalents(self): #last updated 2018-11-21 17:07:08 
		## No primary or unique key
		sql = ("INSERT INTO bible_equivalents(bible_id, equivalent_id, organization_id,"
			" type, site, suffix) VALUES (%s, %s, %s, %s, %s, %s)")
		# Not implemented


	def insertBibleFileTitles(self): #last updated 2018-03-05 03:32:33
		## No primary or unique key
		sql = ("INSERT INTO bible_file_titles(file_id, iso, title, description, key_words)"
			" VALUES (%s, %s, %s, %s, %s)")
		# Not implemented


	def insertBibleLinks(self): #last updated 2019-04-26 15:48:17
		## primary key id is auto increment
		sql = ("INSERT INTO bible_links(bible_id, type, url, title, provider, visible,"
			" organization_id) VALUES (%s, %s, %s, %s, %s, %s, %s)")
		# Not implemented


	def insertBibleOrganizations(self): # last updated 2019-04-30 10:49:05
		## No primary or unique key
		sql = ("INSERT INTO bible_organizations(bible_id, organization_id, relationship_type)"
			" VALUES (%s, %s, %s)")
		# Not implemented


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	update = UpdateDBPDatabase(config)
	update.process(lptsReader)


