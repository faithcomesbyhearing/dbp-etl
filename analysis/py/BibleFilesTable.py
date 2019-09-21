# BibleFilesTable.py
#
#
#| id            | int(10) unsigned    | NO   | PRI | NULL              | auto_increment              |
#| hash_id       | varchar(12)         | NO   | MUL | NULL              |                             |
#| book_id       | char(3)             | NO   | MUL | NULL              |                             |
#| chapter_start | tinyint(3) unsigned | YES  |     | NULL              |                             |
#| chapter_end   | tinyint(3) unsigned | YES  |     | NULL              |                             |
#| verse_start   | tinyint(3) unsigned | YES  |     | NULL              |                             |
#| verse_end     | tinyint(3) unsigned | YES  |     | NULL              |                             |
#| file_name     | varchar(191)        | NO   |     | NULL              |                             |
#| file_size     | int(10) unsigned    | YES  |     | NULL              |                             |
#| duration      | int(10) unsigned    | YES  |     | NULL              |                             |
#
#

import io
import os
import sys
import hashlib
from Config import *
from BucketReader import *
from SQLUtility import *

class BibleFilesTable:

	def __init__(self, config):
		self.config = config
		self.validDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		self.bookIdList = self.validDB.select("SELECT id FROM books", None)
		bucket = BucketReader(config)
		self.filenames = bucket.filenames2()


	def bookId(self, typeCode, fileName):
		bookCode = None
		if typeCode == "aud":
			bookCode = fileName[0:5].strip('_')
		"""
		elif typeCode == "tex":
			parts = fileName.split('.')[0].split('_')
			if len(parts) > 2:
				bookCode = parts[-2]
		elif typeCode == "vid":
			parts = filename.split("_")
			for part in parts:
				if part.count('-')==2: ### this looks really bad, copied from main.py
					parts=part.split("-")
				if part in ["MAT","MRK","LUK","JHN"]:
					bookCode = part
		else:
			sys.exit()
		"""
		return bookCode


	def chapterStart(self, typeCode, filename):
		chapter = None
		if typeCode == "aud":
			chapter = filename[5:9].strip('_')
		elif typeCode == "tex":
			parts = filename.split('.')[0].split('_')
			if len(parts) > 2:
				if parts[-1].isdigit():
					chapter = parts[-1]
		elif typeCode == "vid":
			chapter = None # TBD
		else:
			sys.exit()
		return chapter


	def chapterEnd(self, typeCode, filename):
		return None


	def verseStart(self, typeCode, filename):
		return 1


	def verseEnd(self, typeCode, filename):
		return None


	def fileSize(self, typeCode, filename):
		return None


	def duration(self, typeCode, filename):
		return None


config = Config()
filesets = BibleFilesTable(config) 
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]

	files = filesets.filenames[filesetId]
	for fileName in files:
		#print(filesetId, fileName)

		bookId = filesets.bookId(typeCode, fileName)
		if bookId in filesets.bookIdList:
			chapterStart = filesets.chapterStart(typeCode, fileName)
			chapterEnd = filesets.chapterEnd(typeCode, fileName)
			verseStart = filesets.verseStart(typeCode, fileName)
			verseEnd = filesets.verseEnd(typeCode, fileName)
			fileSize = filesets.fileSize(typeCode, fileName)
			duration = filesets.duration(typeCode, fileName)
			results.append((hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, fileName, fileSize, duration))
		else:
			print("WARNING: Invalid bookId %s in %s" % (bookId, fileName, typeCode))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start, verse_end, file_name, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()



