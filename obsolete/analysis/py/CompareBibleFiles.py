# CompareBibleFiles.py

import sys
import io
import csv
from Config import *
from SQLUtility import *

class CompareBibleFiles:

	def __init__(self, config):
		self.db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		duplicates = self.loadDirectory(config.directory_duplicate)
		quarantine = self.loadDirectory(config.directory_quarantine)
		self.rejects = duplicates.union(quarantine)
		#for item in self.rejects:
		#	print(item)


	def compareNotInTest(self):
		sql = ("SELECT bfc.bible_id, bs.id, p.file_name, p.book_id, p.chapter_start,"
			" p.verse_start, p.hash_id FROM dbp.bible_files p"
			" JOIN dbp.bible_filesets bs ON p.hash_id = bs.hash_id"
			" JOIN dbp.bible_fileset_connections bfc ON bs.hash_id = bfc.hash_id"
			" WHERE NOT EXISTS (SELECT 1 FROM valid_dbp.bible_files t WHERE p.hash_id = t.hash_id"
			#" AND p.book_id = t.book_id AND p.chapter_start = t.chapter_start"
			#" AND p.verse_start = t.verse_start)"
			" AND p.file_name = t.file_name)"
			" AND p.hash_id IN (SELECT t.hash_id"
			" FROM dbp.bible_filesets p, valid_dbp.bible_filesets t"
			" WHERE t.id = p.id)")
		#	#" AND t.set_type_code='video_stream'"
		#	#" AND p.set_type_code='video_stream')")
		resultSet = self.db.select(sql, ())
		results = []
		## I don't think the following is doing anything???
		for row in resultSet:
			key = "%s/%s/%s" % (row[0], row[1], row[2])
			print(key)
			if not key in self.rejects:
				results.append(row)
		print("NUM NOT IN TEST", len(resultSet), len(results))


	def compareNotInProd(self):
		sql = ("SELECT bfc.bible_id, bs.id, p.file_name, p.book_id, p.chapter_start,"
			" p.verse_start, p.hash_id FROM valid_dbp.bible_files p"
			" JOIN valid_dbp.bible_filesets bs ON p.hash_id = bs.hash_id"
			" JOIN valid_dbp.bible_fileset_connections bfc ON bs.hash_id = bfc.hash_id"
			" WHERE NOT EXISTS (SELECT 1 FROM dbp.bible_files t WHERE p.hash_id = t.hash_id"
			#" AND p.book_id = t.book_id AND p.chapter_start = t.chapter_start"
			#" AND p.verse_start = t.verse_start)"
			" AND p.file_name = t.file_name)"
			" AND p.hash_id IN (SELECT t.hash_id"
			" FROM dbp.bible_filesets p, valid_dbp.bible_filesets t"
			" WHERE t.id = p.id)")
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			print(row)
		print("NUM NOT IN PROD", len(resultSet))


	def compareCommon(self):
		bookIdMATErrors = []
		bookIdErrors = []
		chapterStartErrors = []
		chapterEndErrors = []
		verseStartErrors = []
		verseEndErrors = []
		fileSizeErrors = []
		durationErrors = []
		sql = ("SELECT bfc.bible_id, bf.id, t.file_name, t.hash_id,"
			" t.book_id, t.chapter_start, t.chapter_end, t.verse_start, t.verse_end,"
			" t.file_size, t.duration,"
			" p.book_id, p.chapter_start, p.chapter_end, p.verse_start, p.verse_end,"
			" p.file_size, p.duration"
			" FROM valid_dbp.bible_files t, dbp.bible_files p, valid_dbp.bible_filesets bf,"
			" valid_dbp.bible_fileset_connections bfc"
			" WHERE t.hash_id = p.hash_id AND p.file_name = t.file_name"
			" AND bf.hash_id = t.hash_id and bf.hash_id = p.hash_id"
			" AND bfc.hash_id = bf.hash_id")
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			bibleId = row[0]
			filesetId = row[1]
			filename = row[2]
			hashId = row[3]
			testBookId = row[4]
			testChapterStart = row[5]
			testChapterEnd = row[6]
			testVerseStart = row[7]
			testVerseEnd = row[8]
			testFileSize = row[9]
			testDuration = row[10]
			testLabel = "%s:%s:%s" % (testBookId, testChapterStart, testVerseStart)
			prodBookId = row[11]
			prodChapterStart = row[12]
			prodChapterEnd = row[13]
			prodVerseStart = row[14]
			prodVerseEnd = row[15]
			prodFileSize = row[16]
			prodDuration = row[17]
			prodLabel = "%s:%s:%s" % (prodBookId, prodChapterStart, prodVerseStart)


			if testBookId != prodBookId:
				if testBookId == None and prodBookId == "MAT":
					bookIdMATErrors.append((bibleId, filesetId, filename, testLabel, prodLabel))
				else:
					bookIdErrors.append((bibleId, filesetId, filename, testLabel, prodLabel))
			if testChapterStart != prodChapterStart:
				chapterStartErrors.append((bibleId, filesetId, filename, testLabel, prodLabel))
			if testChapterEnd != prodChapterEnd:
				chapterEndErrors.append((bibleId, filesetId, filename, testChapterEnd, prodChapterEnd))
			if testVerseStart != prodVerseStart:
				verseStartErrors.append((bibleId, filesetId, filename, testLabel, prodLabel))
			if testVerseEnd != prodVerseEnd:
				verseEndErrors.append((bibleId, filesetId, filename, testVerseEnd, prodVerseEnd))
			if testFileSize != prodFileSize and prodFileSize != None:
				fileSizeErrors.append((bibleId, filesetId, filename, testFileSize, prodFileSize))
			# if testDuration != prodDuration:
			#	durationErrors.append((bibleId, filesetId, filename, testDuration, prodDuration))

		self.reportErrors("Book id incorrectly assigned MAT in DBP", bookIdMATErrors)
		self.reportErrors("BookId incorrectly assigned in DBP", bookIdErrors)
		self.reportErrors("ChapterStart incorrectly assigned in DBP", chapterStartErrors)
		self.reportErrors("ChapterEnd Errors", chapterEndErrors)
		self.reportErrors("VerseStart incorrectly assigned in DBP", verseStartErrors)
		self.reportErrors("VerseEnd Errors", verseEndErrors)
		self.reportErrors("FileSize Errors", fileSizeErrors)
		self.reportErrors("Duration Errors", durationErrors)
		print("%d file matches found" % (len(resultSet)))


	def loadDirectory(self, dirname):
		results = set()
		files = os.listdir(dirname)
		for file in sorted(files):
			if file.endswith("csv"):
				(bucket, typeCode, bibleId, filesetId) = file.split(".")[0].split("_")
				csvFilename = dirname + os.sep + file
				with open(csvFilename, newline='\n') as csvfile:
					reader = csv.DictReader(csvfile)
					for row in reader:
						filename = row["file_name"]
						key = "%s/%s/%s" % (bibleId, filesetId, filename)
						results.add(key)
		return results


	def reportErrors(self, message, rows):
		print("%d %s" % (len(rows), message))
		for row in rows:
			if len(row) == 5:
				print("  ", row[0], '/', row[1], '/', row[2], "\tnew:", row[3], "\tdbp:", row[4])
			else:
				print(row)
		print("")


config = Config("dev")
compare = CompareBibleFiles(config)
#compare.compareCommon()
#compare.compareNotInTest()
compare.compareNotInProd()


"""
SELECT bfc.bible_id, bf.id, t.file_name, t.hash_id,
t.book_id, t.chapter_start, t.chapter_end, t.verse_start, t.verse_end,
t.file_size, t.duration,
p.book_id, p.chapter_start, p.chapter_end, p.verse_start, p.verse_end
FROM valid_dbp.bible_files t, dbp.bible_files p, valid_dbp.bible_filesets bf,
valid_dbp.bible_fileset_connections bfc
WHERE t.hash_id = p.hash_id AND p.file_name = t.file_name
AND bf.hash_id = t.hash_id and bf.hash_id = p.hash_id
AND bfc.hash_id = bf.hash_id
"""