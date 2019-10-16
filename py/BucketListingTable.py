# BucketListingTable
#
# This program populates bucket_list from a text listing of the buckets

import io
import os
import sys
import re
from Config import *
from LookupTables import *
from SQLUtility import *
from Legacy import *


class BucketListingTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db
		self.lookup = LookupTables()
		self.legacy = Legacy(config)
		self.VIDEO_BOOK_SET = {"MAT","MRK","LUK","JHN"}
		self.VIDEO_BOOK_MAP = {"Mark": "MRK", "MRKZ": "MRK", "Luke": "LUK"}


	def process(self):
		self.createBucketTable()
		self.insertBucketList("dbp-prod")
		self.insertBucketList("dbp-vid")
		self.createIndexes()


	def createBucketTable(self):
		self.db.execute("DROP TABLE IF EXISTS bucket_listing", None)
		sql = ("CREATE TABLE bucket_listing ("
			+ " asset_id varchar(255) not null,"
			+ " type_code varchar(255) not null,"
			+ " bible_id varchar(255) not null,"
			+ " fileset_id varchar(255) not null,"
			+ " file_name varchar(255) not null,"
			+ " set_type_code varchar(255) not null,"
			+ " legacy_asset_id varchar(255) not null,"
			+ " hash_id varchar(255) not null,"
			+ " book_id char(3) NULL,"
			+ " chapter_start varchar(255) NULL,"
			+ " verse_start varchar(255) NULL,"
			+ " verse_end varchar(255) NULL)")
			#+ " video_height varchar(255) NULL)")
		self.db.execute(sql, None)


	def insertBucketList(self, bucketName):
		results = []
		dropTypes = set()
		dropBibleIds = set()
		#dropAppIds = set()
		dropAudioIds = set()
		dropTextIds = set()
		dropVideoIds = set()
		bucketPath = self.config.directory_bucket_list % (bucketName.replace("-", "_"))
		files = io.open(bucketPath, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
				parts = line.strip().split("/")
				if len(parts) == 4:
					typeCode = parts[0]
					bibleId = parts[1]
					filesetId = parts[2]
					fileName = parts[3]
					fileNameSansExt = fileName.split(".")[0]
					row = (bucketName, typeCode, bibleId, filesetId, fileName)
					if bibleId.isupper():
						#if typeCode == "app":
						#	if fileName.endswith(".apk"):
						#		if filesetId.isupper():
						#			setTypeCode = "app"
						#			legacyAssetId = self.legacy.legacyAssetId(filesetId, setTypeCode, bucketName)
						#			hashId = self.legacy.hashId(legacyAssetId, filesetId, setTypeCode)
						#			extra = (setTypeCode, legacyAssetId, hashId)
						#			fileParts = self.parseAppFilename(fileNameSansExt)
						#			results.append(row + extra + fileParts)
						#		else:
						#			dropAppIds.add("app/%s/%s" % (bibleId, filesetId))
						if typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId.isupper():
									setTypeCode = self.setTypeCode(filesetId)
									legacyAssetId = self.legacy.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.legacy.hashId(legacyAssetId, filesetId, setTypeCode)
									extra = (setTypeCode, legacyAssetId, hashId)
									fileParts = self.parseAudioFilename(fileNameSansExt)
									results.append(row + extra + fileParts)
								else:
									dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId.isupper():
									if fileNameSansExt not in {"index", "about"}:
										setTypeCode = "text_format"
										legacyAssetId = self.legacy.legacyAssetId(filesetId, setTypeCode, bucketName)
										hashId = self.legacy.hashId(legacyAssetId, filesetId, setTypeCode)
										extra = (setTypeCode, legacyAssetId, hashId)
										fileParts = self.parseTextFilenames(fileNameSansExt)
										results.append(row + extra + fileParts)
								else:
									dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
						elif typeCode == "video":
							if (fileName.endswith(".mp4") and not fileName.endswith("web.mp4") 
								and not filesetId.endswith("480") and not filesetId.endswith("720")):
								if filesetId.isupper():
									setTypeCode = "video_stream"
									legacyAssetId = self.legacy.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.legacy.hashId(legacyAssetId, filesetId, setTypeCode)
									extra = (setTypeCode, legacyAssetId, hashId)
									fileParts = self.parseVideoFilenames(fileNameSansExt)
									results.append(row + extra + fileParts)
								else:
									dropVideoIds.add("video/%s/%s" % (bibleId, filesetId))
						else:
							dropTypes.add(typeCode)
					else:
						dropBibleIds.add("%s/%s" % (typeCode, bibleId))
		warningPathName = "output/BucketListing_%s.text" % (bucketName)
		output = io.open(warningPathName, mode="w", encoding="utf-8")
		self.privateDrop(output, "WARNING: type_code %s was excluded", dropTypes)
		self.privateDrop(output, "WARNING: bible_id %s was excluded", dropBibleIds)
		#self.privateDrop(output, "WARNING: app_id %s was excluded", dropAppIds)
		self.privateDrop(output, "WARNING: audio_id %s was excluded", dropAudioIds)
		self.privateDrop(output, "WARNING: text_id %s was excluded", dropTextIds)
		self.privateDrop(output, "WARNING: video_id %s was excluded", dropVideoIds)
		output.close()
		print("%d rows to insert" % (len(results)))
		self.db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", results)


	def createIndexes(self):
		self.db.execute("CREATE INDEX bucket_listing_bible_id ON bucket_listing (bible_id)", None)
		self.db.execute("CREATE INDEX bucket_listing_fileset_id ON bucket_listing (fileset_id, asset_id, set_type_code)", None)
		self.db.execute("CREATE INDEX bucket_listing_hash_id ON bucket_listing (hash_id, book_id, chapter_start, verse_start)", None)


	def privateDrop(self, output, message, dropIds):
		print("num %d:  %s" % (len(dropIds), message))
		message += "\n"
		sortedIds = sorted(list(dropIds))
		for dropId in sortedIds:
			output.write(message % (dropId))


	def setTypeCode(self, filesetId):
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
				print("WARNING: file type not known for %s, set_type_code set to 'unknown'" % (filesetId))
				return "unknown"


	#def parseAppFilename(self, fileName):
	#	bookCode = None
	#	chapterStart = "1"
	#	verseStart = "1"
	#	verseEnd = None
	#	#videoHeight = None
	#	return (bookCode, chapterStart, verseStart, verseEnd)#, videoHeight)


	def parseAudioFilename(self, fileName):
		seqCode = fileName[0:3]
		bookCode = self.lookup.bookIdBySequence(seqCode)
		if bookCode == None:
			chapterStart = "1"
		else:
			chapterStart = fileName[5:8].strip("_")
			if not chapterStart.isdigit():
				bookCode = None
				chapterStart = "1"
		verseStart = "1"
		verseEnd = None
		#videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd)#, videoHeight)


	def parseTextFilenames(self, fileName):
		#{fileset}_{seq}_{USFM3}_{chapter}.html (chapter optional)
		#{USFM2}{chapter}.html (chapter optional)
		parts = fileName.split("_")
		if len(parts) > 2:
			bookCode = parts[2]
			chapterStart = parts[3] if len(parts) > 3 else "0"
		else:
			bookCode = self.lookup.bookIdBy2Char(fileName[:2])
			if bookCode == None:
				chapterStart = "1"
			else:
				chapterStart = fileName[2:].strip("_")
				if chapterStart == "":
					chapterStart = "0"
		verseStart = "1"
		verseEnd = None
		#videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd)#, videoHeight)


	def parseVideoFilenames(self, fileName):
		parts = re.split("[_-]", fileName)
		seqCode = fileName[0:3]
		bookCode = self.lookup.bookIdBySequence(seqCode)
		if bookCode != None:
			chapterStart = fileName[5:8].strip("_")
			verseStart = "1"
			verseEnd = None
		else:
			chapterStart = "1"
			verseStart = "1"
			verseEnd = None
			for index in range(len(parts)):
				part = parts[index]
				if len(part) == 3 and part in self.VIDEO_BOOK_SET:
					bookCode = part
				elif len(part) == 4 and part in self.VIDEO_BOOK_MAP.keys():
					bookCode = self.VIDEO_BOOK_MAP[part]
				if bookCode != None:
					chapterStart = parts[index + 1]
					if chapterStart.isdigit():
						verseStart = parts[index + 2] if len(parts) > (index + 2) else "1"
						verseEnd = parts[index + 3] if len(parts) > (index + 3) else None
					break
		#video = parts[-1]
		#if video[:2] == "av" and video[-1:] == "p":
		#	videoHeight = video[2:-1]
		#else:
		#	videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd)#, videoHeight)


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
bucket = BucketListingTable(config, db)
bucket.process()
db.close()

"""
AUDIO BOOK_ID ERRORS
SNMNVSP1DA16 contain USFM code in 2nd term with underscore split
Genesis only

SNMNVSP1DA contains ‘Genesis in 4th term with underscore split
Genesis only

NHXNTVS1DA16 contains book names in spanish
NHXNTVS1DA same

KFTNIES2DA16 contains book name in english in 5th term

ENGNABC1DA contains out of sequence Ann codes
micah is A40, Malachi is A46
has english names in 3rd term

GRKEPTO1DA contains out of sequence Ann codes
A51 is 4 Macabees, A40 is Haggai
has english names in 3rd term
"""


				

