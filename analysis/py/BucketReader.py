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
			bibleList = db.selectList("SELECT distinct bible_id FROM bucket_listing ORDER BY bible_id", None)
			self.bibleIdList = bibleList
		return self.bibleIdList


	def filesetIds(self, typeCode):
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		ids = db.selectList("SELECT distinct fileset_id FROM bucket_listing WHERE type_code = %s", (typeCode))
		db.close()
		return ids


	def filenames(self, typeCode):
		if self.filenameList == None:
			db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
			#hashMap = {}
			self.filenameList = db.selectMapList("SELECT fileset_id, file_name FROM bucket_listing WHERE length(file_name) > 16 and type_code = %s", (typeCode))
			#for parts in files:
			#	if parts[0] not in hashMap:
			#		hashMap[parts[0]] = [parts[1]]
			#	else:
			#		hashMap[parts[0]].append(parts[1])
			db.close()
			#self.filenameList = hashMap
			print("num in bucket %s" % (len(self.filenameList.keys())))
		return self.filenameList


"""
config = Config()
bucket = BucketReader(config)
"""



				

