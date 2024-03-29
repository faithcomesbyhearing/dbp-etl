# BibleFileTimestamps_Insert.py
#
# This program loads timings data into the bible_file_timestamps table.
# It iterates over a series of files in a directory, or over all timings directories in a tree.
#
# Usage: python3 py/BibleFileTimestamps_Insert.py  /top/dir/root/Eng  timing_est_err
# Where the 'Eng' selects all bible directories that start with 'Eng'
# Usage: python3 py/BibleFileTimestamps_Insert.py  /top/dir/root/English KJV  timing_est_err
# Where the 'English' and 'KJV' are part of one string 'Eng KJV'
# Note: the use of wildcard * is NOT expected.

import sys
import os
import io
import re
from SQLUtility import *


TIM_HOST = "localhost"
TIM_USER = "root"
TIM_PORT = 3306
TIM_DB_NAME = "dbp"


class BibleFileTimestamps_Insert:

	def __init__(self):
		self.db = SQLUtility(TIM_HOST, TIM_PORT, TIM_USER, TIM_DB_NAME)
		self.xmlRegex = re.compile(r"<filename src.*>(.*)</filename>")
		self.filenameRegex = re.compile(r"[A-Z0-9]+-[0-9]+-([A-Z1-4]+)-([0-9]+)-timing.txt")
		self.timingRegex = re.compile(r"([0-9\.]+)\t([0-9\.]+)\t([0-9]+)([a-z]?)")


	def getCommandLine(self):
		if len(sys.argv) < 3 or sys.argv[1].find("/") == -1:
			print("Usage: python3 py/BibleFileTimestamps_Insert.py  /top/dir/root/Eng  timing_est_err")
			sys.exit()
		path_and_file = " ".join(sys.argv[1:-1])
		pathDir, pathFile = os.path.split(path_and_file)
		if not os.path.exists(pathDir):
			print("ERROR: Directory %s does not exist" % (pathDir))
			sys.exit()
		timingErr = sys.argv[-1]
		return(pathDir, pathFile, timingErr)


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


	def getHashId(self, filesetId):
		sql = ("SELECT hash_id FROM bible_filesets WHERE asset_id = 'dbp-prod'"
			" AND set_type_code IN ('audio', 'audio_drama')"
			" AND id = %s")
		return self.db.selectScalar(sql, (filesetId))


	def processBible(self, pathDir, bibleDir, timingErr):
		path = pathDir + os.sep + bibleDir + os.sep + bibleDir + ".appDef"
		filesetId = self.getFilesetId(path) 
		path = pathDir + os.sep + bibleDir + os.sep + bibleDir + "_data" + os.sep + "timings"
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
						priorVerse = 0
						for line in timings:
							result = self.timingRegex.match(line)
							timing = round(float(result.group(1)), 2)
							verseStart = int(result.group(3))
							versePart = result.group(4)
							verseSequence = verseStart
							if versePart == "" or versePart == "a":
								values.append((fileId, verseStart + versePart, None, timing, verseSequence))
								# check that all verses are included
								if (priorVerse + 1) != verseStart:
									print("WARNING: %s %s:%s skipped from %d to %d" % (filesetId, book, chapter, priorVerse, verseStart + versePart))
								priorVerse = verseStart
					else:
						print("ERROR: No file for %s %s:%s" % (filesetId, book, chapter))
				else:
					raise Exception("Unable to parse file %s" % (file))

		#for value in values:
		#	print(value)
		deleteStmt = ("DELETE FROM bible_file_timestamps WHERE bible_file_id IN"
			" (SELECT bf.id FROM bible_files bf, bible_filesets bs"
			" WHERE bf.hash_id = bs.hash_id AND bs.id = %s)")
		errorStmt = ("REPLACE INTO bible_fileset_tags (hash_id, name, description, admin_only, iso, language_id)"
			" VALUES (%s, 'timing_est_err', %s, 0, 'eng', 6414)")
		insertStmt = "INSERT INTO bible_file_timestamps (bible_file_id, verse_start, verse_end, `timestamp`, verse_sequence) VALUES (%s, %s, %s, %s, %s)"
		print("Row %d to be inserted for %s, %s" % (len(values), bibleDir, filesetId))
		cursor = self.db.conn.cursor()
		try:
			sql = deleteStmt
			cursor.execute(sql, (filesetId,))
			hashId = self.getHashId(filesetId)
			sql = errorStmt
			cursor.execute(sql, (hashId, timingErr))
			sql = insertStmt
			cursor.executemany(sql, values)
			self.db.conn.commit()
			cursor.close()
		except Exception as err:
			self.db.error(cursor, sql, err)


	def process(self):
		(pathDir, pathFile, timingErr) = self.getCommandLine()
		print(pathDir, pathFile)
		for bibleDir in os.listdir(pathDir):
			if bibleDir.startswith(pathFile):
				self.processBible(pathDir, bibleDir, timingErr)
		self.db.close()


ins = BibleFileTimestamps_Insert()
ins.process()



"""
Script for generating outfile for test comparisons
SELECT bible_file_id, verse_start, verse_end, `timestamp`
FROM bible_file_timestamps WHERE bible_file_id IN
(SELECT bf.id FROM bible_files bf, bible_filesets bs
WHERE bf.hash_id = bs.hash_id AND bs.id = 'ENGKJVN2DA16')
INTO OUTFILE '/tmp/bible_timestamps.out';
"""

"""
Script to test DELETE
SELECT count(*) FROM bible_file_timestamps WHERE bible_file_id IN
(SELECT bf.id FROM bible_files bf, bible_filesets bs
WHERE bf.hash_id = bs.hash_id AND bs.id = 'ENGKJVN2DA16')
"""

