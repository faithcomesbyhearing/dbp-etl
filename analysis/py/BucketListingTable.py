# BucketListingTable
#
# This program populates bucket_list from a text listing of the buckets

import io
import os
import sys
import re
import hashlib
from Config import *
from LookupTables import *
from SQLUtility import *


class BucketListingTable:

	def __init__(self, config):
		self.config = config
		self.lookup = LookupTables()
		self.VIDEO_BOOK_SET = {"MAT","MRK","LUK","JHN"}
		self.VIDEO_BOOK_MAP = {"Mark": "MRK", "MRKZ": "MRK", "Luke": "LUK"}


	def createBucketTable(self):
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		sql = "DROP TABLE IF EXISTS bucket_listing"
		db.execute(sql, None)
		sql = ("CREATE TABLE bucket_listing ("
			+ " bucket varchar(255) not null,"
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
			+ " verse_end varchar(255) NULL,"
			+ " video_height varchar(255) NULL)")
		db.execute(sql, None)
		db.close()


	def insertBucketList(self, bucketName):
		prodDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_input_db_name)
		self.legacyFilesetMap = prodDB.selectMapList("SELECT concat(id, set_type_code), asset_id FROM bible_filesets", None)
		prodDB.close()

		results = []
		dropTypes = set()
		dropBibleIds = set()
		dropAppIds = set()
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
						if typeCode == "app":
							if fileName.endswith(".apk"):
								if filesetId.isupper():
									setTypeCode = "app"
									legacyAssetId = self.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.hashId(legacyAssetId, filesetId, setTypeCode)
									extra = (setTypeCode, legacyAssetId, hashId)
									fileParts = self.parseAppFilename(fileNameSansExt)
									results.append(row + extra + fileParts)
								else:
									dropAppIds.add("app/%s/%s" % (bibleId, filesetId))
						elif typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId.isupper():
									setTypeCode = self.setTypeCode(filesetId)
									legacyAssetId = self.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.hashId(legacyAssetId, filesetId, setTypeCode)
									extra = (setTypeCode, legacyAssetId, hashId)
									fileParts = self.parseAudioFilename(fileNameSansExt)
									results.append(row + extra + fileParts)
								else:
									dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId.isupper():
									setTypeCode = "text_format"
									legacyAssetId = self.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.hashId(legacyAssetId, filesetId, setTypeCode)
									extra = (setTypeCode, legacyAssetId, hashId)
									fileParts = self.parseTextFilenames(fileNameSansExt)
									results.append(row + extra + fileParts)
								else:
									dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
						elif typeCode == "video":
							if fileName.endswith(".m3u8"):
								if filesetId.isupper():
									setTypeCode = "video_stream"
									legacyAssetId = self.legacyAssetId(filesetId, setTypeCode, bucketName)
									hashId = self.hashId(legacyAssetId, filesetId, setTypeCode)
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
		self.privateDrop(output, "WARNING: app_id %s was excluded", dropAppIds)
		self.privateDrop(output, "WARNING: audio_id %s was excluded", dropAudioIds)
		self.privateDrop(output, "WARNING: text_id %s was excluded", dropTextIds)
		self.privateDrop(output, "WARNING: video_id %s was excluded", dropVideoIds)
		output.close()
		print("%d rows to insert" % (len(results)))
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
		db.close()


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


	def legacyAssetId(self, filesetId, setTypeCode, assetId):
		key = filesetId + setTypeCode
		legacyIds = self.legacyFilesetMap.get(key)
		if legacyIds == None:
			#print("no legacy bucket found actual one returned")
			return assetId
		elif len(legacyIds) == 1:
			return legacyIds[0]
		else:
			#print("multiple Ids %s  %s  %s" % (filesetId, setTypeCode, ",".join(legacyIds)))
			# When there are two the first is always dbp-prod, the second is dbp-dbs
			return legacyIds[0]


	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]	


	def parseAppFilename(self, fileName):
		bookCode = None
		chapterStart = None
		verseStart = None
		verseEnd = None
		videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd, videoHeight)


	def parseAudioFilename(self, fileName):
		seqCode = fileName[0:3]
		bookCode = self.lookup.bookIdBySequence(seqCode)
		if bookCode == None:
			chapterStart = None # might not be necessary
		else:
			chapterStart = fileName[5:8].strip("_")
			if not chapterStart.isdigit():
				bookCode = None
				chapterStart = None
		verseStart = 1
		verseEnd = None
		videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd, videoHeight)


	def parseTextFilenames(self, fileName):
		parts = fileName.split("_")
		if len(parts) > 2:
			bookCode = parts[2]
			chapterStart = parts[1]
		else:
			bookCode = self.lookup.bookIdBy2Char(fileName[:2])
			if bookCode == None:
				chapterStart = None
			else:
				chapterStart = fileName[2:].strip("_")
				if chapterStart == "":
					chapterStart = "0"
		verseStart = 1
		verseEnd = None
		videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd, videoHeight)


	def parseVideoFilenames(self, fileName):
		parts = re.split("[_-]", fileName)
		seqCode = fileName[0:3]
		bookCode = self.lookup.bookIdBySequence(seqCode)
		if bookCode != None:
			chapterStart = fileName[5:8].strip("_")
			verseStart = None
			verseEnd = None
		else:
			chapterStart = None
			verseStart = None
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
						verseStart = parts[index + 2]
						verseEnd = parts[index + 3]
					break
		video = parts[-1]
		if video[:2] == "av" and video[-1:] == "p":
			videoHeight = video[2:-1]
		else:
			videoHeight = None
		return (bookCode, chapterStart, verseStart, verseEnd, videoHeight)


config = Config()
bucket = BucketListingTable(config)
bucket.createBucketTable()
bucket.insertBucketList("dbp-prod")
bucket.insertBucketList("dbp-vid")

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


				

