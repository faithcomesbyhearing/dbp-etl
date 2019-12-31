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
import csv
from Config import *


class InputReader:

	def __init__(self, config):
		self.config = config
		self.results = {}


	## This method is probably only for testing
	def typeCodeListing(self, typeCode):
		if typeCode == "video":
			return self.bucketListing(self.config.s3_vid_bucket)
		else:
			self.bucketListing(self.config.s3_bucket)
			results2 = {}
			for key in self.results.keys():
				if key.startswith(typeCode):
					results2[key] = self.results[key]
			self.results = results2
			self.countSummary()
			return self.results


	def bucketListing(self, bucketName):
		dropTypes = set()
		dropBibleIds = set()
		dropAudioIds = set()
		dropTextIds = set()
		dropVideoIds = set()
		#dropNot4Parts = set()
		ignoredFiles = []
		bucketPath = self.config.directory_bucket_list + bucketName + ".txt"
		files = io.open(bucketPath, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
			#if "audio/ENG" in line:   ####### debug
				parts = line.strip().split("/", 4)
				#if len(parts) == 4:
				if len(parts) == 4 and "/" not in parts[3]:
					typeCode = parts[0]
					bibleId = parts[1]
					filesetId = parts[2]
					fileName = parts[3]
					fileNameSansExt = fileName.split(".")[0]
					key = "%s/%s/%s" % (typeCode, bibleId, filesetId)
					if bibleId.isupper():
						if typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId.isupper():
									self.appendResults(key, fileName)
								else:
									dropAudioIds.add(key)
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							else:
								ignoredFiles.append(parts + ["audio is not mp3"])
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId.isupper():
									if fileNameSansExt not in {"index", "about"}:
										self.appendResults(key, fileName)
									else:
										ignoredFiles.append(parts + ["ignored meta data file"])
								else:
									dropTextIds.add(key)
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							else:
								ignoredFiles.append(parts + ["text is not .html"])
						elif typeCode == "video":
							if (fileName.endswith(".mp4") and not fileName.endswith("web.mp4") 
								and not filesetId.endswith("480") and not filesetId.endswith("720")):
								if filesetId.isupper():
									self.appendResults(key, fileName)
								else:
									dropVideoIds.add(key)
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							else:
								ignoredFiles.append(parts + ["video is not .mp4"])
						else:
							dropTypes.add(typeCode)
							ignoredFiles.append(parts + ["not audio; text; video"])
					else:
						dropBibleIds.add("%s/%s" % (typeCode, bibleId))
						ignoredFiles.append(parts + ["bible_id is not uppercase"])
				else:
					#dropNot4Parts.add("%s/%s/%s" % (typeCode, bibleId, filesetId))
					ignoredFiles.append(parts + ["Not 4 parts"])

		warningPathName = self.config.directory_errors + "InputReaderErrors_%s.text" % (bucketName)
		output = io.open(warningPathName, mode="w", encoding="utf-8")
		self.privateDrop(output, "WARNING: type_code %s was excluded", dropTypes)
		self.privateDrop(output, "WARNING: bible_id %s was excluded", dropBibleIds)
		self.privateDrop(output, "WARNING: audio_id %s was excluded", dropAudioIds)
		self.privateDrop(output, "WARNING: text_id %s was excluded", dropTextIds)
		self.privateDrop(output, "WARNING: video_id %s was excluded", dropVideoIds)
		output.close()
		self.writeIgnoredCSV(ignoredFiles)
		print("start countSummary", len(self.results.keys()))
		self.countSummary()
		return self.results


	def privateDrop(self, output, message, dropIds):
		print("num %d:  %s" % (len(dropIds), message))
		message += "\n"
		sortedIds = sorted(list(dropIds))
		for dropId in sortedIds:
			output.write(message % (dropId))


	def appendResults(self, key, filename):
		files = self.results.get(key, [])
		files.append(filename)
		self.results[key] = files


	def writeIgnoredCSV(self, ignoredFiles):
		print("IgnorFiles", len(ignoredFiles))
		filename = self.config.directory_errors + "IgnoredFiles.csv"
		with open(filename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(("typeCode", "bible_id", "fileset_id", "file_name", "error"))
			for row in ignoredFiles:
				writer.writerow(row)


	def countSummary(self):
		print("%d filesets found" % (len(self.results.keys())))
		count = 0
		for files in self.results.values():
			count += len(files)
		print("%d files found" % (count))
		return self.results


#config = Config("dev")
#reader = InputReader(config)
#reader.bucketListing('dbp-vid')
#reader.bucketListing('dbp-prod')
#reader.typeCodeListing('audio')
#reader.typeCodeListing('text')




				

