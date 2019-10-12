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
from SQLUtility import *

class BibleFilesTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()

		results = []
		uniqueSet = set()

		for fileset in self.bucketList:
			fileName = fileset[0]
			hashId = fileset[1]
			bookId = fileset[2]
			chapterStart = fileset[3]
			if chapterStart == "End":
				if bookId == "MAT":
					chapterStart = "28"
					verseStart = "21"
					verseEnd = "21"
				elif bookId == "MRK":
					chapterStart = "16"
					verseStart = "21"
					verseEnd = "21"
				elif bookId == "LUK":
					chapterStart = "24"
					verseStart = "54"
					verseEnd = "54"
				elif bookId == "JHN":
					chapterStart = "21"
					verseStart = "26"
					verseEnd = "26"
				else:
					chapterStart = "0"
					verseStart = None
					verseEnd = None
			chapterEnd = None
			verseStart = fileset[4]
			if verseStart != None:
				if "b" in verseStart:
					verseStart = verseStart.replace("b", "")
			verseEnd = fileset[5]
			if verseEnd != None:
				if "r" in verseEnd:
					verseEnd = verseEnd.replace("r", "")
				if "a" in verseEnd:
					verseEnd = verseEnd.replace("a", "")
			fileSize = None
			duration = None

			key = "%s-%s-%s-%s" % (hashId, bookId, chapterStart, verseStart)
			if key in uniqueSet:
				print("WARNING: dropping duplicate key ", fileset)
			elif hashId not in self.hashIdSet:
				print("WARNING: dropping hash_id mismatch ", fileset)
			elif chapterStart == "0" and fileName[-5:] == ".html":
				# This drop is probably not correct for production, correct feedback from App devs
				print("WARNING: dropping zero chapter html files")
			else:
				uniqueSet.add(key)
				results.append((hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, fileName, fileSize, duration))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start, verse_end, file_name, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", results)


	def readAll(self):
		sql = ("SELECT file_name, hash_id, book_id, chapter_start, verse_start, verse_end"
			+ " FROM bucket_listing"
			+ " WHERE book_id IS NOT NULL AND chapter_start is NOT NULL"
			+ " AND book_id IN (SELECT id FROM books)"
			+ " ORDER by hash_id, book_id, chapter_start, verse_start, length(file_name) DESC, file_name")
		self.bucketList = self.db.select(sql, None)
		print("num %d files rows in bucket_listing" % (len(self.bucketList)))
		self.hashIdSet = self.db.selectSet("SELECT hash_id FROM bible_filesets", None)


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
files = BibleFilesTable(config, db)
files.process()
db.close() 


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

