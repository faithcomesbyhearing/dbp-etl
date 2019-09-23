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


	def bibleIds(self):
		if self.bibleIdList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			bibleList = db.selectList("SELECT distinct bible_id FROM bucket_listing WHERE type_code IN ('app', 'audio', 'text', 'video') ORDER BY bible_id", None)
			self.bibleIdList = bibleList
		return self.bibleIdList


	def filesetIds(self, typeCode):
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		ids = db.selectList("SELECT distinct fileset_id FROM bucket_listing WHERE type_code = %s", (typeCode))
		db.close()
		return ids


	def filenames(self):
		if self.filenameList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			hashMap = {}
			files = db.select("SELECT type_code, bible_id, fileset_id, part_3, part_4, part_5, part_6 FROM bucket_listing WHERE num_parts = 4 AND type_code IN ('app', 'audio', 'text', 'video')", None)
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
"""



				

