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
		self.versesFilename = "bible_verses.text"
		self.bibleIdList = None
		self.filesetIdList = None
		self.filesetBibleIdList = None


	def bibleIds(self):
		if self.bibleIdList == None:
			bibles = set()
			file = io.open(self.versesFilename, mode="r", encoding="utf-8")
			for line in file:
				parts = line.split("/")
				bibles.add(parts[1])
			file.close()
			self.bibleIdList = sorted(list(bibles))
		return self.bibleIdList


	def filesetIds(self):
		if self.filesetIdList == None:
			filesets = set()
			file = io.open(self.versesFilename, mode="r", encoding="utf-8")
			for line in file:
				parts = line.split("/")
				filesets.add(parts[2])
			file.close()
			self.filesetIdList = sorted(list(filesets))
		return self.filesetIdList


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

"""
config = Config()
reader = VersesReader(config)
ids = reader.bibleIdFilesetId()
for id1 in ids:
	print(id1)
"""



				

