# BucketReader
#
# This table has various ways to read a bucket listing.

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
		files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
				parts = line.strip().split("/")
				if len(parts) == 4:
					typeCode = parts[0]
					bibleId = parts[1]
					filesetId = parts[2]
					fileName = parts[3]
					#if typeCode in ["app", "audio", "text", "video"]:
					if bibleId in bibleIdMap:
						if typeCode == "app":
							if filesetId in audioMap:
								self.privateAddRow(results, parts)
							else:
								dropAppIds.add("app/%s/%s" % (bibleId, filesetId))
						elif typeCode == "audio":
							if filesetId[:10] in audioMap:
								self.privateAddRow(results, parts)
							else:
								dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
						elif typeCode == "text":
							if filesetId in textMap:
								self.privateAddRow(results, parts)
							else:
								dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
						elif typeCode == "video":
							if filesetId in videoMap:
								self.privateAddRow(results, parts)
							else:
								dropVideoIds.add("video/%s/%s" % (bibleId, filesetId))
						#elif typeCode in ["bibles.json", "fonts", "languages"]:
						#	self.privateAddRow(results, parts)
						else:
							dropTypes.add(typeCode)
					else:
						dropBibleIds.add("%s/%s" % (typeCode, bibleId))
		self.privateDrop("WARNING: type_code %s is excluded", dropTypes)
		self.privateDrop("WARNING: bible_id %s was not found in LPTS", dropBibleIds)
		self.privateDrop("WARNING: app_id %s was not found in LPTS", dropAppIds)
		self.privateDrop("WARNING: audio_id %s was not found in LPTS", dropAudioIds)
		self.privateDrop("WARNING: text_id %s was not found in LPTS", dropTextIds)
		self.privateDrop("WARNING: video_id %s was not found in LPTS", dropVideoIds)
		print("%d rows to insert" % (len(results)))
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s)", results)
		db.close()


	def privateAddRow(self, results, parts):
		row = [None] * 5
		row[0] = "dbp-prod"
		for index in range(len(parts)):
			row[index + 1] = parts[index]
		results.append(row)


	def privateDrop(self, message, dropIds):
		for dropId in dropIds:
			print(message % (dropId))


config = Config()
bucket = BucketListingTable(config)
bucket.createBucketTable()




				

