# BucketListingTable
#
# This program populates bucket_list from a text listing of the buckets

import io
import os
import sys
from Config import *
from SQLUtility import *
from LPTSExtractReader import *


class BucketListingTable:

	def __init__(self, config):
		self.config = config


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
			+ " file_name varchar(255) not null)")
		db.execute(sql, None)
		db.close()

	def insertBucketList(self, bucketName):
		reader = LPTSExtractReader(self.config)
		bibleIdMap = reader.getBibleIdMap()
		print("num bible ids in LPTS", len(bibleIdMap.keys()))
		audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(audioMap.keys()))
		textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(textMap.keys()))
		videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(videoMap.keys()))
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
					if bibleId in bibleIdMap:
						if typeCode == "app":
							if fileName.endswith(".apk"):
								if filesetId in audioMap:
									self.privateAddRow(results, parts, bucketName)
								else:
									dropAppIds.add("app/%s/%s" % (bibleId, filesetId))
						elif typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId[:10] in audioMap:
									self.privateAddRow(results, parts, bucketName)
								else:
									dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId in textMap:
									self.privateAddRow(results, parts, bucketName)
								else:
									dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
						elif typeCode == "video":
							if fileName.endswith(".m3u8"):
								if filesetId in videoMap:
									self.privateAddRow(results, parts, bucketName)
								else:
									dropVideoIds.add("video/%s/%s" % (bibleId, filesetId))
						else:
							dropTypes.add(typeCode)
					else:
						dropBibleIds.add("%s/%s" % (typeCode, bibleId))
		warningPathName = "output/BucketListing_%s.text" % (bucketName)
		output = io.open(warningPathName, mode="w", encoding="utf-8")
		self.privateDrop(output, "WARNING: type_code %s is excluded", dropTypes)
		self.privateDrop(output, "WARNING: bible_id %s was not found in LPTS", dropBibleIds)
		self.privateDrop(output, "WARNING: app_id %s was not found in LPTS", dropAppIds)
		self.privateDrop(output, "WARNING: audio_id %s was not found in LPTS", dropAudioIds)
		self.privateDrop(output, "WARNING: text_id %s was not found in LPTS", dropTextIds)
		self.privateDrop(output, "WARNING: video_id %s was not found in LPTS", dropVideoIds)
		output.close()
		print("%d rows to insert" % (len(results)))
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s)", results)
		db.close()


	def privateAddRow(self, results, parts, bucketName):
		row = [None] * 5
		row[0] = bucketName
		for index in range(len(parts)):
			row[index + 1] = parts[index]
		results.append(row)


	def privateDrop(self, output, message, dropIds):
		print("num %d:  %s" % (len(dropIds), message))
		message += "\n"
		sortedIds = sorted(list(dropIds))
		for dropId in sortedIds:
			output.write(message % (dropId))


config = Config()
bucket = BucketListingTable(config)
bucket.createBucketTable()
bucket.insertBucketList("dbp-prod")
bucket.insertBucketList("dbp-vid")
#bucket.insertBucketList("dbs-web")




				

