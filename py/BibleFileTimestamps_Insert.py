# BibleFileTimestamps_Insert.py
#
# This program loads timings data into the bible_file_timestamps table.
# It iterates over a series of files in a directory, or over all timings directories in a tree.
#
# Usage: python3 py/BibleFileTimestamps_Insert.py  bible_id or ALL
#
# 

import sys
import os
import io
import re
from SQLUtility import *


TIM_HOST = "localhost"
TIM_USER = "root"
TIM_PORT = 3306
TIM_DB_NAME = "dbp"
TIM_MP3_DIRECTORY = "/Volumes/FCBH/DBP_v2_data"


class BibleFileTimestamps_Insert:

	def __init__(self):
		self.db = SQLUtility(TIM_HOST, TIM_PORT, TIM_USER, TIM_DB_NAME)
		self.xmlRegex = re.compile(r"<filename src.*>(.*)</filename>")
		self.filenameRegex = re.compile(r"[A-Z0-9]+-[0-9]+-([A-Z1-4]+)-([0-9]+)-timing.txt")


	def getCommandLine(self):
		if len(sys.argv) < 2:
			print("Usage: python3 py/BibleFileTimestamps_Insert.py  bible_id or ALL")
			sys.exit()
		return " ".join(sys.argv[1:])


	def getFilesetId(self, path):
		filesetId = None
		xml = io.open(path, mode="r")
		for line in xml:
			if line.strip().startswith("<filename src"):
				audioPath = self.xmlRegex.search(line)
				parts = audioPath.group(1).split("\\")
				result = parts[-2]
				if filesetId == None:
					filesetId = result
				elif filesetId != result:
					print("ERROR: multiple filsetid in appDev file", filesetId, result)
		return filesetId + "16"


	def getFileId(self, filesetId, bookId, chapter):
		sql = ("SELECT bf.id FROM bible_files bf, bible_filesets bs"
			" WHERE bf.hash_id =  bs.hash_id"
			" AND bs.id = %s"
			" AND bf.book_id = %s"
			" AND bf.chapter_start = %s"
			" AND bf.verse_start = 1")
		return self.db.selectScalar(sql, (filesetId, bookId, int(chapter)))
		# This codes does not work for SNMNVSP1DA, because it has verse_start other than 1
		# Others might have this in the future.
		# Possibly we need a query that returns verse_start or verse_start and verse_end


	def processBible(self, bible):
		path = TIM_MP3_DIRECTORY + os.sep + bible + os.sep + bible + ".appDef"
		filesetId = self.getFilesetId(path) 
		path = TIM_MP3_DIRECTORY + os.sep + bible + os.sep + bible + "_data" + os.sep + "timings"
		values = []
		for file in sorted(os.listdir(path)):
			if not file.startswith("."):
				result = self.filenameRegex.match(file)
				if result != None:
					book = result.group(1)
					chapter = result.group(2)
					fileId = self.getFileId(filesetId, book, chapter)
					if fileId != None:
						timings = io.open(path + os.sep + file, mode="r")
						for line in timings:
							parts = line.strip().split("\t")
							timing = round(float(parts[0]), 2)
							values.append((fileId, int(parts[2]), None, timing))
					else:
						print("ERROR: No file for %s %s:%s" % (filesetId, book, chapter))
				else:
					raise Exception("Unable to parse file %s" % (file))
		sql = "INSERT INTO bible_file_timestamps (bible_file_id, verse_start, verse_end, `timestamp`) VALUES (%s, %s, %s, %s)"
		#for value in values:
		#	print(value)
		print("Row %d to be inserted for %s" % (len(values), bible))
		self.db.executeBatch(sql, values)


	def process(self):
		bible = self.getCommandLine()
		if bible.upper() == "ALL":
			for bible in os.listdir(TIM_MP3_DIRECTORY):
				self.processBible(bible)
		else:
			self.processBible(bible)
		self.db.close()


ins = BibleFileTimestamps_Insert()
ins.process()


