# BucketReader
#
# This table has various ways to read a bucket listing.

import io
import os
import sys
from Config import *
from SQLUtility import *


class BucketReader:

	def __init__(self, config):
		self.config = config
		self.bibleIdList = None
		self.audioIdList = None
		self.textIdList = None
		self.videoIdList = None
		self.filenameList = None

	def createBucketTable(self):
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		sql = "DROP TABLE IF EXISTS bucket_listing"
		db.execute(sql, None)
		sql = ("CREATE TABLE bucket_listing ("
			+ " num_parts int,"
			+ " typeCode varchar(255) not null,"
			+ " bible_id varchar(255) null,"
			+ " fileset_id varchar(255) null,"
			+ " part_3 varchar(255) null,"
			+ " part_4 varchar(255) null,"
			+ " part_5 varchar(255) null,"
			+ " part_6 varchar(255) null,"
			+ " part_7 varchar(255) null)")
		db.execute(sql, None)
		results = []
		files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
				parts = line.strip().split("/")
				row = [None] * 9
				row[0] = len(parts)
				for index in range(len(parts)):
					row[index + 1] = parts[index]
				results.append(row)
		print("%d rows to insert" % (len(results)))
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
		db.close()



	def bibleIds(self):
		if self.bibleIdList == None:
			ids = set()
			files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
			for line in files:
				parts = line.split("/")
				if parts[0] in ["audio", "text", "video"]:
					if len(parts) > 3:
						bibleId = parts[1]
						if "delete" not in bibleId:
							#print(bibleId)
							ids.add(bibleId)
			files.close()
			self.bibleIdList = sorted(list(ids))
		return self.bibleIdList


	def filesetIds(self):
		if self.audioIdList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			bibleIds = db.selectList("SELECT id FROM bibles", None)
			db.close()
			audIds = set()
			txtIds = set()
			vidIds = set()
			files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
			for line in files:
				parts = line.split("/")
				if len(parts) > 3 and parts[1] in bibleIds:
					#print("found", parts[0], parts[1], parts[2])
					if parts[0] == "audio":
						audIds.add(parts[2])
					elif parts[0] == "text":
						txtIds.add(parts[2])
					elif parts[0] == "video":
						vidIds.add(parts[2])
			files.close()
			self.audioIdList = sorted(list(audIds))
			self.textIdList = sorted(list(txtIds))
			self.videoIdList = sorted(list(vidIds))
			print("num in bucket audio=%d  text=%d  video=%d" % (len(audIds), len(txtIds), len(vidIds)))

	def filenames(self):
		if self.filenameList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			bibleIds = db.selectList("SELECT id FROM bibles", None)
			filesetIds = db.selectList("SELECT id FROM bible_filesets", None)
			db.close()
			hashMap = {}
			files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
			for line in files:
				parts = line.strip().split("/")
				if len(parts) > 3 and parts[1] in bibleIds:
					if parts[2] in filesetIds:
						#print("found", parts[0], parts[1], parts[2])
						if parts[2] not in hashMap:
							hashMap[parts[2]] = [parts[-1]]
						else:
							hashMap[parts[2]].append(parts[-1])
			files.close()
			self.filenameList = hashMap
			print("num in bucket %s" % (len(hashMap.keys())))
		return self.filenameList


	def filenames2(self):
		if self.filenameList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			bibleIds = db.selectList("SELECT id FROM bibles", None)
			filesetIds = db.selectList("SELECT id FROM bible_filesets", None)
			db.close()
			hashMap = {}
			#files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
			files = db.select("SELECT typeCode, bible_id, fileset_id, part_3, part_4, part_5, part_6 FROM bucket_listing limit 20000")
			for parts in files:
				#parts = line.strip().split("/")
				if len(parts) > 3 and parts[1] in bibleIds:
					if parts[2] in filesetIds:
						#print("found", parts[0], parts[1], parts[2])
						if parts[2] not in hashMap:
							hashMap[parts[2]] = [parts[-1]]
						else:
							hashMap[parts[2]].append(parts[-1])
			files.close()
			self.filenameList = hashMap
			print("num in bucket %s" % (len(hashMap.keys())))
		return self.filenameList



config = Config()
bucket = BucketReader(config)
bucket.createBucketTable()




				

