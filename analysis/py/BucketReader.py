# BucketReader
#
# This table has various ways to read a bucket listing.

import io
import os
import sys
from Config import *

class BucketReader:

	def __init__(self, config):
		self.config = config
		self.bibleIdList = None


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