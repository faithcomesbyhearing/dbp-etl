# BucketListingTable
#
# This program populates bucket_list from a text listing of the buckets

import io
import os
import sys
from Config import *
from LookupTables import *
from SQLUtility import *


class BucketListingTable:

	def __init__(self, config):
		self.config = config
		self.lookup = LookupTables()


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
			+ " book_id char(3) NULL)")
		db.execute(sql, None)
		db.close()

	def insertBucketList(self, bucketName):
		results = []
		dropTypes = set()
		dropBibleIds = set()
		dropAppIds = set()
		dropAudioIds = set()
		dropTextIds = set()
		dropVideoIds = set()
		dropFilenames = set()
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
					row = (bucketName, typeCode, bibleId, filesetId, fileName)
					if bibleId.isupper():
						if len(fileName) > 3:
							if typeCode == "app":
								if fileName.endswith(".apk"):
									if filesetId.isupper():
										fileParts = self.parseAppFilename(fileName)
										results.append(row + fileParts)
									else:
										dropAppIds.add("app/%s/%s" % (bibleId, filesetId))
							elif typeCode == "audio":
								if fileName.endswith(".mp3"):
									if filesetId.isupper():
										fileParts = self.parseAudioFilename(fileName)
										results.append(row + fileParts)
									else:
										dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
							elif typeCode == "text":
								if fileName.endswith(".html"):
									if filesetId.isupper():
										fileParts = self.parseTextFilenames(fileName)
										results.append(row + fileParts)
									else:
										dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
							elif typeCode == "video":
								if fileName.endswith(".m3u8"):
									if filesetId.isupper():
										fileParts = self.parseVideoFilenames(fileName)
										results.append(row + fileParts)
									else:
										dropVideoIds.add("video/%s/%s" % (bibleId, filesetId))
							else:
								dropTypes.add(typeCode)
						else:
							dropFilenames.add("/".join(row))
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
		self.privateDrop(output, "WARNING: file_name %s is too short, was excluded", dropFilenames)
		output.close()
		print("%d rows to insert" % (len(results)))
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s, %s)", results)
		db.close()


	def privateDrop(self, output, message, dropIds):
		print("num %d:  %s" % (len(dropIds), message))
		message += "\n"
		sortedIds = sorted(list(dropIds))
		for dropId in sortedIds:
			output.write(message % (dropId))


	def parseAppFilename(self, fileName):
		bookCode = None
		return (bookCode,)


	def parseAudioFilename(self, fileName):
		seqCode = fileName[0:3]
		bookCode = self.lookup.bookIdBySequence(seqCode)
		return (bookCode,)


	def parseTextFilenames(self, fileName):
		bookCode = None
		parts = fileName.split('.')[0].split('_')
		if len(parts) > 2:
			bookCode = parts[2]
		else:
			bookCode = self.lookup.bookIdBy2Char(fileName[:2])
		return (bookCode,)


	def parseVideoFilenames(self, fileName):
		bookCode = None
		parts = fileName.split("_")
		for index in range(len(parts)):
			part = parts[index]
			if len(part) == 3 and part in {"MAT","MRK","LUK","JHN"}:
				bookCode = part
				break
			if len(part) == 4:
				if part == "Mark":
					bookCode = "MRK"
					break
				elif part == "Luke":
					bookCode = "LUK"
					break
				elif part == "MRKZ":
					bookCode = "MRK"
					break
		if bookCode == None:
			seqCode = fileName[0:3]
			bookCode = self.lookup.bookIdBySequence(seqCode)			
		return (bookCode,)


config = Config()
bucket = BucketListingTable(config)
bucket.createBucketTable()
bucket.insertBucketList("dbp-prod")
bucket.insertBucketList("dbp-vid")
#bucket.insertBucketList("dbs-web")




				

