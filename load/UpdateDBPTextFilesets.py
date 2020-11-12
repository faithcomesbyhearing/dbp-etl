# UpdateDBPTextFilesets.py

# Processing data in the following format
# text/{bibleId}/{filesetId}/options.xini
# text/{bibleId}/{filesetId}/{book}.usx (all the usx files to process)

import io
import sys
import os
import sqlite3
from Config import *
from LPTSExtractReader import *
from SQLUtility import *
from SQLBatchExec import *
from S3Utility import *


class UpdateDBPTextFilesets:

	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader


	## This is called one fileset at a time by Validate.process around line 82
	def validateFileset(self, bibleId, filesetId):
		(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose("text", bibleId, filesetId)
		if lptsRecord == None:
			return("text/%s/%s has no LPTS record.\tEROR" % (bibleId, filesetId))
		iso3 = lptsRecord.ISO()
		if iso3 == None:
			return("text/%s/%s has no iso language code.\tEROR" % (bibleId, filesetId))
		iso1 = self.db.selectScalar("SELECT iso1 FROM languages WHERE iso = %s AND iso1 IS NOT NULL", (iso3,))
		scriptName = lptsRecord.Orthography(lptsIndex)
		if scriptName == None:
			return("text/%s/%s has no Orthography.\tEROR" % (bibleId, filesetId))
		scriptId = self.db.selectScalar("SELECT script_id FROM lpts_script_codes WHERE lpts_name = %s", (scriptName,))
		if scriptId == None:
			return("text/%s/%s %s script name is not in lpts_script_codes.\tEROR" % (bibleId, filesetId, scriptName))
		direction = self.db.selectScalar("SELECT direction FROM alphabets WHERE script = %s", (scriptId,))
		if direction not in {"ltr", "rtl"}:
			return("text/%s/%s %s script has no direction in alphabets.\tEROR" % (bibleId, filesetId, scriptId))
			sys.exit()
		cmd = [self.config.node_exe,
			self.config.mysql_exe,
			self.config.directory_validate,
			self.config.directory_accepted,
			iso3, iso1, direction]
		print(cmd)
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=120)
		success = response.returncode == 0
		if success != None:
			print("text/%s/%s %s\tEROR" % (bibleId, filesetId, str(response.stderr.decode("utf-8"))))
		print("text/%s/%s %s\tINFO" % (bibleId, filesetId, str(response.stderr.decode("utf-8"))))
		print("OUTPUT", str(response.stdout.decode("utf-8")))	
		return None
		
		# This must return error messages and error count by some mechanism


	def uploadFileset(self, bibleId, filesetId):
		# At some future time the process might extract .html chapter files from the {fileset}.db database and upload them.
		# At some future time the process might upload .usx files.
		# Or, at some future time, the process might create json equivalents of the usx and upload them.
		s3 = S3Utility(self.config)
		s3.promoteFileset(self, self.config.directory_upload, "text/%s/%s" % (bibleId, filesetId))


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
				if dbpChapters != ssChapterList:
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
	def insertBibleBooksIfAbsent(self, filesetPrefix):
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
	s3 = S3Utility(config)
	dirname = config.directory_validate
	for typeCode in os.listdir(dirname):
		if typeCode == "text":
			for bibleId in os.listdir(dirname + typeCode):
				for filesetId in os.listdir(dirname + typeCode + os.sep + bibleId):
					error = texts.validateFileset(bibleId, filesetId)
					if error == None:
						s3.promoteFileset(self, self.config.directory_validation, "text/%s/%s" % (bibleId, filesetId))
						texts.uploadFileset(bibleId, filesetId)

	dirname = config.directory_database
	for typeCode in os.listdir(dirname):
		if typeCode == "text":
			for bibleId in os.listdir(dirname + typeCode):
				for filesetId in os.listdir(dirname + typeCode + os.sep + bibleId):
					databasePath = config.directory_accepted + filesetId + ".db"
					hashId = fileset.insertBibleFileset("verses", filesetId, databasePath)
					fileset.insertFilesetConnections(hashId, bibleId)
					text.updateFileset(bibleId, filesetId, hashId)
	dbOut.displayCounts()
	dbOut.displayStatements()
	#dbOut.execute()




