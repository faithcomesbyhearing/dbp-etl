# UpdateDBPTextFilesets.py

# Processing data in the following format
# text/{bibleId}/{filesetId}/options.xini
# text/{bibleId}/{filesetId}/{book}.usx (all the usx files to process)

import io
import sys
import os
import sqlite3
#from xml.dom import minidom
from Config import *


class UpdateDBPTextFilesets:

	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader


	## This is called one fileset at a time by Validate.process around line 82
	def validateFileset(self, bibleId, filesetId):
		print("TBD")
		# The options.xini file will be read to obtain the filesetId and the languageId code.
		# The filesetId in options.xini must be equal to the filesetId of the directory, or it will produce an error and stop processing this fileset.
		# The languageId code in the options.xini must be equal iso code in LPTS for the fileset, or it will produce and error and stop processing this fileset.
		# The process will lookup the iso1 code that corresponds to the iso language code using the languages table.
		# The process will use the LPTS orthography code, and look it up in the alphabets table to get the direction of the language
		# The process will compare the LPTS orthography with the orthography allowed for the language using the language_alphabets table, and produce and error and stop the process if they do not match.
		# The process will execute my Bible text processing program using the above parameters.
		# The output of the process is a sqlite database with the name {filesetId}.db
		# Possibly, the process could derive the exact script code from the text and compare to the the LPTS orthography, and stop the process if it is not correct.
		# It will place this database file in the the accepted directory (I think)
		# This must return error messages and error count by some mechanism


	def uploadFileset(self, bibleId, filesetId):
		print("Text Filesets are not uploaded")
		# At some future time the process might extract .html chapter files from the {fileset}.db database and upload them.
		# At some future time the process might upload .usx files.
		# Or, at some future time, the process might create json equivalents of the usx and upload them.
		# At this time, there is nothing the upload process must do


	def updateFileset(self, bibleId, filesetId, hashId):
		bibleDB = connect(self.config.directory_accepted + filesetId + ".db")
		cursor = bibleDB.cursor()
		self.updateBibleBooks(cursor, bibleId)
		self.updateVerseTable(cursor, hashId)
		cursor.close()
		bibleDB.close()


	def updateBibleBooks(self, cursor, bibleId):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = "SELECT book_id, name, name_short, chapters FROM bible_books WHERE bible_id = %s"
		resultSet = self.db.select(sql, (bibleId,))
		dbpBooksMap = {}
		for (dbpBookId, dbpName, dbpNameShort, dbpChapters) in resultSet:
			dbpBooksMap[dbpBookId] = (dbpName, dbpNameShort, dbpChapters)

		ssBookIdSet = set()
		for (ssBookId, ssTitle, ssName, ssLastChapter) in cursor.execute("SELECT code, title, name, lastChapter FROM tableContents"):
			ssBookIdSet.add(ssBookId)
			chapterList = map(str, range(firstChapter, lastChapter))
			ssChapterList =  ",".join(chapterList)

			if ssBookId not in dbpBooksMap.keys():
				insertRows.append((ssTitle, ssName, ssChapterList, bibleId, ssBookId))
			else:
				(dbpName, dbpNameShort, dbpChapters) = dbpBooksMap[ssBookId]
				if dbpName != ssTitle:
					updateRows.append(("name", ssTitle, dbpName, bibleId, ssBookId))
				if dbpNameShort != ssName:
					updateRows.append(("name_short", ssName, dbpNameShort, bibleId, ssBookId))
				if dbpChapters != ssChapterList
					updateRows.append(("chapters", ssChapterList, dbpChapters, bibleId, ssBookId))

		for dbpBookId in dbpBookMap.keys():
			if dbpBookId not in ssBookIdSet:
				deleteRows.append((bibleId, dbpBookId))

		tableName = "bible_books"
		pkeyNames = ("bible_id", "book_id")
		attrNames = ("name", "name_short", "chapters")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)



	def updateVersesTable(self, cursor, hashId):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = "SELECT book_id, chapter, verse_start, verse_end, verse_text FROM bible_verses WHERE hash_id = %s"
		resultSet = self.db.select(sql, (hashId,))
		dbpVerseMap = {}
		for (dbpBookId, dbpChapter, dbpVerseStart, dbpVerseEnd, dbpVerseText) in resultSet:
			dbpVerseMap[(dbpBookId, dbpChapter, dbpVerseStart)] = (dbpVerseEnd, dbpVerseText)

		ssBookIdSet = set()
		for (ssReference, ssVerseText) in cursor.execute("SELECT reference, html FROM verses"):
			(ssBookId, ssChapter, ssVerse) = ssReference.split(":")
			ssBookIdSet.add((ssBookId, ssChapter, ssVerse))

		if (ssBookIdSet, ssChapter, ssVerse) not in dbpVerseMap.keys():
			insertRows.append((ssVerse, ssVerseText, hashId, ssBookId, ssChapter, ssVerse))
		else:
			(dbpVerseEnd, dbpVerseText) = dbpVerseMap.get((ssBookIdSet, ssChapter, ssVerse))
			if dbpVerseEnd != ssVerse:
				updateRows.append(("verse_end", ssVerse, dbpVerseEnd, hashId, ssBookId, ssChapter, ssVerse))
			if dbpVerseText != ssVerseText:
				updateRows.append(("verse_text", ssVerseText, dbpVerseText, hashId, ssBookId, ssChapter, ssVerse))

		for (dbpBookId, dbpChapter, dbpVerseStart) in dbpBookMap.keys():
			if (dbpBookId, dbpChapter, dbpVerseStart) not in ssBookIdSet:
				deleteRows.append((hashId, (dbpBookId, dbpChapter, dbpVerseStart)))

		tableName = "bible_verses"
		pkeyNames = ("hash_id", "book_id", "chapter", "verse_start")
		attrNames = ("verse_end", "verse_test")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	## Update table with bible books data from audio or video fileset
	def createIfAbsent(self, filesetPrefix):
		(typeCode, bibleId, filesetId) = filesetPrefix.split("/")
		if typeCode in {"audio", "video"}:
			bookCount = db.selectScalar("SELECT count(*) FROM bible_books WHERE bible_id = %s", (bibleId,))
			if bookCount == 0:
				bookIdList = []
				bookChapterMap = {}
				priorBookId = None
				priorChapter = None
				csvFilename = self.config.directory_accepted + filesetPrefix.replace("/", "_") + ".csv"
				with open(csvFilename, newline='\n') as csvfile:
					reader = csv.DictReader(csvfile)
					for row in reader:
						bookId = row["book_id"]
						chapter = row["chapter_start"]
						if bookId != priorBookId:
							bookIdList.append(bookId)
							bookChapterMap[bookId] = [chapter]
						elif chapter != priorChapter:
							chapters = bookChapterMap[bookId]
							chapters.append("," + chapter)
							bookChapterMap[bookId] = chapters
						priorBookId = bookId
						priorChapter = chapter

				insertRows = []
				for bookId in bookIdList:
					bookName = self.bookNameMap.get(bookId)
					chapters = bookChapterMap[bookId]
					insertRows.append((bookName, bookName, chapters, bibleId, bookId))

				tableName = "bible_books"
				pkeyNames = ("bible_id", "book_id")
				attrNames = ("name", "name_short", "chapters")
				self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)



if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	texts = UpdateDBPTextFilesets(config, db, dbOut, lptsReader)
	dirname = self.config.directory_database
	for typeCode in os.listdir(dirname):
		for bibleId in os.listdir(dirname + typeCode):
			for filesetId in os.listdir(dirname + typeCode + os.sep + bibleId):
				csvFilename = config.directory_accepted + typeCode + "_" + bibleId + "_" + filesetId + ".csv"
				if os.exists(csvFilename):
					if typeCode == "sql":
						hashId = "abcdefg" ### test only
						text.updateVerses(dirname, hashId, bibleId, filesetId)
					elif typeCode == "bookNames":
						text.updateBookNames(dirname, bibleId)
	dbOut.displayCounts()
	dbOut.displayStatements()
	#dbOut.execute()




