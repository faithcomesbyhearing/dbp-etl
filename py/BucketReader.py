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


#	def bibleIds(self):
#		if self.bibleIdList == None:
#			db = SQLUtility(self.config.database_host, self.config.database_port,
#				self.config.database_user, self.config.database_output_db_name)
#			bibleList = db.selectList("SELECT distinct bible_id FROM bucket_listing ORDER BY bible_id", None)
#			self.bibleIdList = bibleList
#			db.close()
#		return self.bibleIdList


	def filesetIds(self, typeCode):
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		ids = db.selectList("SELECT distinct fileset_id FROM bucket_listing WHERE type_code = %s", (typeCode))
		db.close()
		return ids


#	def filesets(self):
#		db = SQLUtility(self.config.database_host, self.config.database_port,
#			self.config.database_user, self.config.database_output_db_name)
#		fileset = db.select("SELECT distinct fileset_id, bucket, type_code FROM bucket_listing", None)
#		db.close()
#		return fileset	


	def filenames(self, typeCode):
		db = SQLUtility(self.config.database_host, self.config.database_port,
				self.config.database_user, self.config.database_output_db_name)
		#Can I control the order of the files?
		#filenameList = db.selectMapList("SELECT fileset_id, file_name FROM bucket_listing WHERE length(file_name) > 16 and type_code = %s", (typeCode))
		filenameList = db.selectMapList("SELECT fileset_id, file_name FROM bucket_listing WHERE type_code = %s", (typeCode))
		db.close()
		print("num in bucket %s" % (len(filenameList.keys())))
		return filenameList


"""
config = Config()
bucket = BucketReader(config)
"""



				

