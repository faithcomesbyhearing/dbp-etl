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
from Config import *
from LookupTables import *
from BucketReader import *
from SQLUtility import *

class BibleFilesTable:

	def __init__(self, config):
		self.config = config


	def readAll(self):
		# 1. read bucket listing in sequence
		# cons: This solution will require comparison to determine when the filesetid or bibleid has changed
		# cons: It will loose the first occurrance of a 2nd bible_id
		# pros: It will guarantee that each line of the bucket is processed
		# 2. read the fileset listing in sequence
		# cons: requires building tables of the filenames
		self.lookup = LookupTables()
		validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesetList = validDB.select("SELECT id, set_type_code, asset_id, hash_id FROM bible_filesets WHERE set_type_code != 'text_plain'", None)
		print("num %d filesets in bible_filesets table" % (len(self.filesetList)))
		self.bookIdSet = set(validDB.selectList("SELECT id FROM books", None))
		print("num %d books in bookList" % len(self.bookIdSet))
		bucket = BucketReader(self.config)
		self.appFilenames = bucket.filenames("app")
		print("num %d app files in bucket" % (len(self.appFilenames)))
		self.audioFilenames = bucket.filenames("audio")
		print("num %d audio files in bucket" % (len(self.audioFilenames)))
		self.textFilenames = bucket.filenames("text")
		print("num %d text files in bucket" % (len(self.textFilenames)))
		self.videoFilenames = bucket.filenames("video")
		print("num %d video files in bucket" % (len(self.videoFilenames)))
		validDB.close()


	def bookId(self, typeCode, fileName):
		bookCode = None
		if typeCode == "app":
			bookCode == "MAT" # Repeating a bug in current program
		elif typeCode == "aud":
			seqCode = fileName[0:3]
			bookCode = self.lookup.bookIdBySequence(seqCode)
		elif typeCode == "tex":
			parts = fileName.split('.')[0].split('_')
			if len(parts) > 2:
				bookCode = parts[2]
		elif typeCode == "vid":
			print(fileName)
			parts = fileName.split("_")
			print(parts)
			for part in parts:
				print(len(part), part)
				if len(part) == 3 and part in {"MAT","MRK","LUK","JHN"}:
					bookCode = part
					print(bookCode)
		else:
			print("ERROR: unknown typeCode %s in bookId()" % (typeCode))
			sys.exit()
		return bookCode


	def chapterStart(self, typeCode, filename):
		chapter = None
		if typeCode == "app":
			chapter = None
		elif typeCode == "aud":
			parts = filename.split("_")
			for part in parts:
				if part.isdigit():
					chapter = (part)
					break
		elif typeCode == "tex":
			parts = filename.split('.')[0].split('_')
			if len(parts) > 3:
				if parts[3].isdigit():
					chapter = (parts[3])
		elif typeCode == "vid":
			parts = fileName.split("_")
			for index in range(len(parts)):
				part = parts[index]
				if len(part) == 3 and part in {"MAT","MRK","LUK","JHN"}:
					part = parts[index + 1]
					pieces = part.split("-")
					if len(pieces) == 3 and pieces[0].isdigit() and pieces[1].isdigit() and pieces[2].isdigit():
						chapter = pieces
		else:
			sys.exit()
		return chapter


	def chapterEnd(self, typeCode, filename):
		# null is intended
		return None


	def verseStart(self, typeCode, filename):
		return "1"


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
filesets.readAll()
results = []
bookIdErrorCount = 0
bookIdNotExistCount = 0
dupErrorCount = 0
chapterErrorCount = 0
uniqueSet = set()

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	bucket = fileset[2]
	hashId = fileset[3]

	if typeCode == "app":
		files = filesets.appFilenames.get(filesetId)
	elif typeCode == "aud":
		files = filesets.audioFilenames.get(filesetId)
	elif typeCode == "tex":
		files = filesets.textFilenames.get(filesetId)
	elif typeCode == "vid":
		files = filesets.videoFilenames.get(filesetId)
	else:
		print("ERROR: unknown typeCode %s" % (typeCode))
		sys.exit()

	if files != None:
		for fileName in files:

			bookId = filesets.bookId(typeCode, fileName)
			if bookId != None:
				if bookId in filesets.bookIdSet:
					chapterStart = filesets.chapterStart(typeCode, fileName)
					if chapterStart != None and len(chapterStart) == 3:
						verseStart = chapterStart[2]
						chapterEnd = chapterStart[1]
						chapterStart = chapterStart[0]
						#if chapterStart < 152:
					else:
						if chapterStart != None:
							chapterStart = chapterStart[0]
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
				#else:
				#	print("WARNING in chapterStart %d in %s" % (chapterStart, fileName))
				#	chapterErrorCount += 1
				else:
					print("WARNING: bookId %s in %s is not in books table" % (bookId, fileName))
					bookIdNotExistCount += 1				
			else:
				print("WARNING: Invalid bookId in %s  %s" % (fileName, typeCode))
				bookIdErrorCount += 1
	else:
		print("ERROR: filenames not found for filesetId %s  type %s" % (filesetId, typeCode))
		sys.exit()

print("num invalid bookId errors dropped %d" % (bookIdErrorCount))
print("num bookId not in books table dropped %d" % (bookIdNotExistCount))
print("num duplicate records dropped %d" % (dupErrorCount))
print("num invalid chapter numbers dropped %d" % (chapterErrorCount))
print("num records to insert %d" % (len(results)))

#filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start, verse_end, file_name, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()

"""
app
book_id = MAT
chapter_start NULL
chapter_end NULL
verse_start NULL
verse_end NULL
file_name = fileName
file_size = actual
duration = NULL

audio and audio_drama
book_id = actual
chapter_start = actual
chapter_end = NULL
verse_start = actual
verse_end = NULL
file_name = actual
file_size = actual
duration = actual

text_format
audio and audio_drama
book_id = actual
chapter_start = actual
chapter_end = NULL
verse_start = actual
verse_end = NULL
file_name = actual
file_size = NULL
duration = NULL

text_plain
like text_format, but has only one fileset SIGWBT, text_plain, dbs-web, hidden 0

video_stream
book_id = actual
chapter_start = actual
chapter_end = NULL
verse_start = actual
verse_end = actual
file_name = actual
file_size = NULL
duration = actual
"""

