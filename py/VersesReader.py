# VersesReader
#
# This table has various ways to read the bible_verses.text file

import io
import os
import sys
from Config import *


class VersesReader:

	def __init__(self, config):
		self.config = config
		self.versesFilename = self.config.directory_bucket_list % ("bible_verses")
#		self.bibleIdList = None
#		self.filesetIdList = None
		self.filesetBibleIdList = None


#	def bibleIds(self):
#		if self.bibleIdList == None:
#			bibles = set()
#			file = io.open(self.versesFilename, mode="r", encoding="utf-8")
#			for line in file:
#				parts = line.split("/")
#				bibles.add(parts[1])
#			file.close()
#			self.bibleIdList = sorted(list(bibles))
#		return self.bibleIdList


#	def filesetIds(self):
#		if self.filesetIdList == None:
#			filesets = set()
#			file = io.open(self.versesFilename, mode="r", encoding="utf-8")
#			for line in file:
#				parts = line.split("/")
#				filesets.add(parts[2])
#			file.close()
#			self.filesetIdList = sorted(list(filesets))
#		return self.filesetIdList

	## This method is used in BucketVerseSummaryTable.  If there are no other uses of this class
	## move this method to that class.
	def bibleIdFilesetId(self):
		if self.filesetBibleIdList == None:
			filesets = set()
			file = io.open(self.versesFilename, mode="r", encoding="utf-8")
			for line in file:
				parts = line.split("/")
				filesets.add(parts[1] + "/" + parts[2])
			file.close()
			self.filesetBibleIdList = sorted(list(filesets))
		return self.filesetBibleIdList


#	def filesetBibleMapList(self):
#		filesets = {}
#		file = io.open(self.versesFilename, mode="r", encoding="utf-8")
#		for line in file:
#			parts = line.split("/")
#			bibleId = parts[1]
#			filesetId = parts[2]
#			ids = filesets.get(filesetId, [])
#			if bibleId not in ids:
#				ids.append(bibleId)
#				filesets[filesetId] = ids
#		return filesets




"""
config = Config()
reader = VersesReader(config)
ids = reader.bibleIdFilesetId()
for id1 in ids:
	print(id1)
"""



				

