# BibleBooksTable.py
#
#
#| bible_id   | varchar(12)  | NO   | PRI | NULL              |
#| book_id    | char(3)      | NO   | PRI | NULL              |
#| name       | varchar(191) | NO   |     | NULL              |
#| name_short | varchar(191) | NO   |     | NULL              |
#| chapters   | varchar(491) | NO   |     | NULL              |
#
#

import io
import os
import sys
#import hashlib
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


	def bibleId(self):
		return None


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


	def name(self, typeCode, filename):
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


	def nameShort(self, typeCode, filename):
		return None


	def chapters(self, typeCode, filename):
		return None


print("This program is not finished, or even really started")
print("I think to update this we need to use info.json files")
sys.exit()

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
		bibleId = ??
		bookId = filesets.bookId(typeCode, fileName)
		if bookId in filesets.bookIdList:
			name = filesets.name(typeCode, fileName)
			nameShort = filesets.nameShort(typeCode, fileName)
			chapters = filesets.chapters(typeCode, fileName)
			results.append((bibleId, bookId, name, nameShort, chapters))
		else:
			print("WARNING: Invalid bookId %s in %s" % (bookId, fileName, typeCode))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_books (bible_id, book_id, name, name_short, chapters) VALUES (%s, %s, %s, %s, %s)", results)
outputDB.close()



