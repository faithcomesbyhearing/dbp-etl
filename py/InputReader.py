# InputReader.py
#
# The InputReader class contains a set of iterators or generators for each kind of input 
# so that the remaining processing uses the same logic for any input.  
# There is be an iterator for all files, and one for just those that will be loaded into bible_files. 
# That is, filenames with 3 slashes whose types are html, mp3, and mp4.  
# The iterator works at two levels, i.e. fileset and file so that there is a natural 
# to organize logic around the beginning and ending of a fileset.

import io
import os
import sys
import re
from Config import *


class InputReader:

	def __init__(self, config):
		self.config = config
		self.results = {}


	def bucketListing(self, bucketName):
		dropTypes = set()
		dropBibleIds = set()
		dropAudioIds = set()
		dropTextIds = set()
		dropVideoIds = set()
		bucketPath = self.config.directory_bucket_list % (bucketName.replace("-", "_"))
		files = io.open(bucketPath, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
				parts = line.strip().split("/")
				if len(parts) == 4:
					typeCode = parts[0]
					bibleId = parts[1]
					filesetId = parts[2]
					fileName = parts[3]
					fileNameSansExt = fileName.split(".")[0]
					row = (typeCode, bibleId, filesetId, fileName)
					if bibleId.isupper():
						if typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId.isupper():
									self.appendResults(filesetId, row)
								else:
									dropAudioIds.add("audio/%s/%s" % (bibleId, filesetId))
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId.isupper():
									if fileNameSansExt not in {"index", "about"}:
										self.appendResults(filesetId, row)
								else:
									dropTextIds.add("text/%s/%s" % (bibleId, filesetId))
						elif typeCode == "video":
							if (fileName.endswith(".mp4") and not fileName.endswith("web.mp4") 
								and not filesetId.endswith("480") and not filesetId.endswith("720")):
								if filesetId.isupper():
									self.appendResults(filesetId, row)
								else:
									dropVideoIds.add("video/%s/%s" % (bibleId, filesetId))
						else:
							dropTypes.add(typeCode)
					else:
						dropBibleIds.add("%s/%s" % (typeCode, bibleId))	
		warningPathName = "output/InputReaderErrors_%s.text" % (bucketName)
		output = io.open(warningPathName, mode="w", encoding="utf-8")
		self.privateDrop(output, "WARNING: type_code %s was excluded", dropTypes)
		self.privateDrop(output, "WARNING: bible_id %s was excluded", dropBibleIds)
		self.privateDrop(output, "WARNING: audio_id %s was excluded", dropAudioIds)
		self.privateDrop(output, "WARNING: text_id %s was excluded", dropTextIds)
		self.privateDrop(output, "WARNING: video_id %s was excluded", dropVideoIds)
		output.close()
		self.countSummary()


	def privateDrop(self, output, message, dropIds):
		print("num %d:  %s" % (len(dropIds), message))
		message += "\n"
		sortedIds = sorted(list(dropIds))
		for dropId in sortedIds:
			output.write(message % (dropId))


	def appendResults(self, filesetId, row):
		files = self.results.get(filesetId, [])
		files.append(row)
		self.results[filesetId] = files


	def countSummary(self):
		print("%d filesets found" % (len(self.results.keys())))
		count = 0
		for files in self.results.values():
			count += len(files)
		print("%d files found" % (count))
		return self.results


config = Config()
reader = InputReader(config)
reader.bucketListing('dbp-prod')




				

