# BucketReader
#
# This table has various ways to read a bucket listing.

import io
import os
import sys
from Config import *
from SQLUtility import *
from LPTSExtractReader import *


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
			+ " type_code varchar(255) not null,"
			+ " bible_id varchar(255) null,"
			+ " fileset_id varchar(255) null,"
			+ " part_3 varchar(255) null,"
			+ " part_4 varchar(255) null,"
			+ " part_5 varchar(255) null,"
			+ " part_6 varchar(255) null,"
			+ " part_7 varchar(255) null)")
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
				typeCode = parts[0]
				bibleId = parts[1] if len(parts) > 1 else None
				filesetId = parts[2] if len(parts) > 2 else None
				if typeCode in ["app", "audio", "text", "video"] and bibleId in bibleIdMap:
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
					elif typeCode in ["bibles.json", "fonts", "languages"]:
						self.privateAddRow(results, parts)
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
		db.executeBatch("INSERT INTO bucket_listing VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
		db.close()


	def privateAddRow(self, results, parts):
		row = [None] * 9
		row[0] = len(parts)
		for index in range(len(parts)):
			row[index + 1] = parts[index]
		results.append(row)


	def privateDrop(self, message, dropIds):
		for dropId in dropIds:
			print(message % (dropId))


	def bibleIds(self):
		if self.bibleIdList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			bibleList = db.selectList("SELECT distinct bible_id FROM bucket_listing WHERE type_code IN ('app', 'audio', 'text', 'video') ORDER BY bible_id", None)
			self.bibleIdList = bibleList
		return self.bibleIdList

	def bibleIds_obsolete(self):
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

	def filesetIds(self, typeCode):
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		ids = db.selectList("SELECT distinct fileset_id FROM bucket_listing WHERE type_code = %s", (typeCode))
		db.close()
		return ids	


	def filesetIds_obslete(self):
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

	def filenames_obsolete(self):
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


	def filenames(self):
		if self.filenameList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			hashMap = {}
			files = db.select("SELECT type_code, bible_id, fileset_id, part_3, part_4, part_5, part_6 FROM bucket_listing WHERE type_code IN ('app', 'audio', 'text', 'video')", None)
			for parts in files:
				if parts[2] not in hashMap:
					hashMap[parts[2]] = [parts[3]]
				else:
					hashMap[parts[2]].append(parts[3])
			db.close()
			self.filenameList = hashMap
			print("num in bucket %s" % (len(hashMap.keys())))
		return self.filenameList


"""
config = Config()
bucket = BucketReader(config)
bucket.createBucketTable()
"""



				

