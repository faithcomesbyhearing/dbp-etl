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
from LookupTables import *
from BucketReader import *
from SQLUtility import *

class BibleFilesTable:

	def __init__(self, config):
		self.config = config
		self.lookup = LookupTables()
		self.validDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, asset_id, hash_id FROM bible_filesets", None)
		print("num %d filesets in bible_filesets table" % (len(self.filesetList)))
		self.bucket = BucketReader(config)


	def bookId(self, typeCode, fileName):
		bookCode = None
		if typeCode == "aud":
			seqCode = fileName[0:3]
			bookCode = self.lookup.bookIdBySequence(seqCode)
		elif typeCode == "tex":
			parts = fileName.split('.')[0].split('_')
			if len(parts) > 2:
				bookCode = parts[2]
		elif typeCode == "vid":
			parts = filename.split("_")
			for part in parts:
				if part.count('-')==2: ### this looks really bad, copied from main.py
					parts=part.split("-")
				if part in ["MAT","MRK","LUK","JHN"]:
					bookCode = part
		else:
			sys.exit()
		return bookCode


	def chapterStart(self, typeCode, filename):
		chapter = 0
		if typeCode == "aud":
			parts = filename.split("_")
			for part in parts:
				if part.isdigit():
					chapter = int(part)
					break
		elif typeCode == "tex":
			parts = filename.split('.')[0].split('_')
			if len(parts) > 3:
				if parts[3].isdigit():
					chapter = int(parts[3])
		elif typeCode == "vid":
			chapter = 0 # TBD
		else:
			sys.exit()
		return int(chapter)


	def chapterEnd(self, typeCode, filename):
		# null is intended
		return None


	def verseStart(self, typeCode, filename):
		return 1


	def verseEnd(self, typeCode, filename):
		return None


	def fileSize(self, typeCode, filename):
		# to be done
		return None


	def duration(self, typeCode, filename):
		# to be done
		return None


config = Config()
filesets = BibleFilesTable(config) 
results = []
bookIdErrorCount = 0
bookIdNotExistCount = 0
dupErrorCount = 0
chapterErrorCount = 0
uniqueSet = set()

bookIdList = filesets.validDB.selectList("SELECT id FROM books", None)

audioFilenames = filesets.bucket.filenames("audio")
print("num %d audio files in bucket" % (len(audioFilenames)))
textFilenames = filesets.bucket.filenames("text")
print("num %d text files in bucket" % (len(textFilenames)))

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	bucket = fileset[2]
	hashId = fileset[3]

	if typeCode == "aud":
		files = audioFilenames.get(filesetId, [])
		#files = []
	elif typeCode == "tex":
		files = textFilenames.get(filesetId, [])
	else:
		files = []
	for fileName in files:

		bookId = filesets.bookId(typeCode, fileName)
		if bookId != None:
			if bookId in bookIdList:
				chapterStart = filesets.chapterStart(typeCode, fileName)
				if chapterStart < 152:
					chapterEnd = filesets.chapterEnd(typeCode, fileName)
					verseStart = filesets.verseStart(typeCode, fileName)
					verseEnd = filesets.verseEnd(typeCode, fileName)
					fileSize = filesets.fileSize(typeCode, fileName)
					duration = filesets.duration(typeCode, fileName)
					key = "%s-%s-%s-%s" % (hashId, bookId, chapterStart, verseStart)
					if key in uniqueSet:
						print("WARNING: duplicate key %s in %s/?/%s/%s" % (key, typeCode, filesetId, fileName))
						dupErrorCount += 1
					else:
						uniqueSet.add(key)
						results.append((hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, fileName, fileSize, duration))
				else:
					print("WARNING in chapterStart %d in %s" % (chapterStart, fileName))
					chapterErrorCount += 1
			else:
				print("WARNING: bookId %s in %s is not in books table" % (bookId, fileName))
				bookIdNotExistCount += 1				
		else:
			print("WARNING: Invalid bookId in %s  %s" % (fileName, typeCode))
			bookIdErrorCount += 1

print("num invalid bookId errors dropped %d" % (bookIdErrorCount))
print("num bookId not in books table dropped %d" % (bookIdNotExistCount))
print("num duplicate records dropped %d" % (dupErrorCount))
print("num invalid chapter numbers dropped %d" % (chapterErrorCount))
print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start, verse_end, file_name, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()



