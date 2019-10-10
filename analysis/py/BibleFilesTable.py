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

	def __init__(self, config):
		self.config = config


	def readAll(self):
		validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		sql = ("SELECT file_name, hash_id, book_id, chapter_start, verse_start, verse_end"
			+ " FROM bucket_listing"
			+ " WHERE book_id IS NOT NULL AND chapter_start is NOT NULL"
			+ " AND book_id IN (SELECT id FROM books)"
			+ " ORDER by hash_id, book_id, chapter_start, verse_start, length(file_name) DESC, file_name")
		self.bucketList = validDB.select(sql, None)
		print("num %d files rows in bucket_listing" % (len(self.bucketList)))
		self.hashIdSet = validDB.selectSet("SELECT hash_id FROM bible_filesets", None)
		validDB.close()


config = Config()
files = BibleFilesTable(config) 
files.readAll()

results = []
uniqueSet = set()

for fileset in files.bucketList:
	fileName = fileset[0]
	hashId = fileset[1]
	bookId = fileset[2]
	chapterStart = fileset[3]
	if chapterStart == "End":
		chapterStart = "0"
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
	elif hashId not in files.hashIdSet:
		print("WARNING: dropping hash_id mismatch ", fileset)
	else:
		uniqueSet.add(key)
		results.append((hashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, fileName, fileSize, duration))

print("num records to insert %d" % (len(results)))
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

