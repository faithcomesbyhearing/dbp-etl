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
		self.filesetIdList = None


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
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		bibleIds = db.selectList("SELECT id FROM bibles", None)
		db.close()
		if self.filesetIdList == None:
			ids = set()
			files = io.open(self.config.directory_main_bucket, mode="r", encoding="utf-8")
			for line in files:
				parts = line.split("/")
				if parts[0] in ["audio", "text"]:
					if parts[1] in bibleIds:
						#print("found", parts[0], parts[1], parts[2])
						ids.add(parts[2])
			files.close()
			self.filesetIdList = sorted(list(ids))
		return self.filesetIdList

				

