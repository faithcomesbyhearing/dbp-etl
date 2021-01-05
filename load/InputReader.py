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
from datetime import datetime
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


	## input: listing of files in a bucket
	## output: map: typeCode/bibleId/filesetId: [(filename, fileSize, datetime)]
	def bucketListing(self, bucketName):
		ignoredFiles = []
		bucketPath = self.config.directory_bucket_list + bucketName + ".txt"
		files = io.open(bucketPath, mode="r", encoding="utf-8")
		for line in files:
			if "delete" not in line:
				(fname, length, datetime) = line.strip().split("\t")
				parts = fname.split("/", 4)
				if len(parts) == 4 and "/" not in parts[3]:
					typeCode = parts[0]
					bibleId = parts[1]
					filesetId = parts[2]
					fileName = parts[3]
					parts.append(datetime)
					fileNameSansExt = fileName.split(".")[0]
					key = "%s/%s/%s" % (typeCode, bibleId, filesetId)
					if bibleId.isupper():
						if typeCode == "audio":
							if fileName.endswith(".mp3"):
								if filesetId.isupper():
									self.appendResults(key, fileName, length, datetime)
								else:
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							else:
								ignoredFiles.append(parts + ["audio is not mp3"])
						elif typeCode == "text":
							if fileName.endswith(".html"):
								if filesetId.isupper():
									if fileNameSansExt not in {"index", "about"}:
										self.appendResults(key, fileName, length, datetime)
									else:
										ignoredFiles.append(parts + ["ignored meta data file"])
								else:
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							else:
								ignoredFiles.append(parts + ["text is not .html"])
						elif typeCode == "video":
							if (fileName.endswith(".mp4") and not fileName.endswith("web.mp4") 
								and not filesetId.endswith("480") and not filesetId.endswith("720")):
								if filesetId.isupper():
									self.appendResults(key, fileName, length, datetime)
								else:
									ignoredFiles.append(parts + ["fileset_id is not uppercase"])
							#else:
								#ignoredFiles.append(parts + ["video is not .mp4"])
						else:
							ignoredFiles.append(parts + ["not audio; text; video"])
					else:
						ignoredFiles.append(parts + ["bible_id is not uppercase"])
				else:
					ignoredFiles.append(parts + ["Not 4 parts"])

		self.writeIgnoredCSV(bucketName, ignoredFiles)
		print("start countSummary", len(self.results.keys()))
		self.countSummary()
		return self.results


	def appendResults(self, key, filename, length, datetime):
		files = self.results.get(key, [])
		files.append((filename, length, datetime))
		self.results[key] = files


	def writeIgnoredCSV(self, bucketName, ignoredFiles):
		print("IgnoredFiles", len(ignoredFiles))
		filename = self.config.directory_errors + bucketName + "_IgnoredFiles.csv"
		with open(filename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(("typeCode", "bible_id", "fileset_id", "file_name", "datetime", "error"))
			for row in ignoredFiles:
				writer.writerow(row)


	## input: audio, text, and video files in a directory tree
	## output: map: typeCode/bibleId/filesetId: [(filename, fileSize, datetime)]
	def fileListing(self, directory):
		lenDirectory = len(directory)
		for root, dirs, files in os.walk(directory):
			relDirName = root[lenDirectory:]
			if relDirName.count("/") == 2 or relDirName.count("\\") == 2:
				if relDirName[:4] in {"audi", "text", "vide"}:
					normDirName = relDirName.replace("\\", "/")
					resultFileList = []
					for file in files:
						if not file.startswith("."):
							if file not in {"about.html", "index.html", "info.json", "title.json", "metadata.xml"}:
								filePath = root + os.sep + file
								filesize = os.path.getsize(filePath)
								modifiedTS = os.path.getmtime(filePath)
								lastModified = datetime.fromtimestamp(modifiedTS)
								resultFileList.append((file, filesize, lastModified))
					self.results[normDirName] = sorted(resultFileList)
		return self.results


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



