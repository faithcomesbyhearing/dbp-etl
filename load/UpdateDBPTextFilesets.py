# UpdateDBPTextFilesets.py


import io
import sys
import os
import sqlite3
from Config import *
from Log import *
from LPTSExtractReader import *
from SQLUtility import *
from SQLBatchExec import *
from S3Utility import *
from SqliteUtility import *


class UpdateDBPTextFilesets:

	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut


	## This is called one fileset at a time by Validate.process
	def validateFileset(self, bibleId, filesetId, lptsRecord, lptsIndex, fullFilesetPath):
		if lptsRecord == None or lptsIndex == None:
			return((Log.EROR, "has no LPTS record."))
		iso3 = lptsRecord.ISO()
		if iso3 == None:
			return((Log.EROR, "has no iso language code."))
		iso1 = self.db.selectScalar("SELECT iso1 FROM languages WHERE iso = %s AND iso1 IS NOT NULL", (iso3,))
		if iso1 == None:
			iso1 = "null"
		scriptName = lptsRecord.Orthography(lptsIndex)
		if scriptName == None:
			return((Log.EROR, "has no Orthography."))
		scriptId = self.db.selectScalar("SELECT script_id FROM lpts_script_codes WHERE lpts_name = %s", (scriptName,))
		if scriptId == None:
			return((Log.EROR, "%s script name is not in lpts_script_codes." % (scriptName,)))
		direction = self.db.selectScalar("SELECT direction FROM alphabets WHERE script = %s", (scriptId,))
		if direction not in {"ltr", "rtl"}:
			return((Log.EROR, "%s script has no direction in alphabets." % (scriptId,)))
		cmd = [self.config.node_exe,
			self.config.publisher_js,
			fullFilesetPath,
			self.config.directory_accepted,
			filesetId, iso3, iso1, direction]
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=120)
		if response == None or response.returncode != 0:
			return((Log.EROR, "BiblePublisher: " + str(response.stderr.decode("utf-8"))))
		#print("text/%s/%s %s\tINFO" % (bibleId, filesetId, str(response.stdout.decode("utf-8"))))
		print("BiblePublisher:", str(response.stdout.decode("utf-8")))	
		return None


	def updateFileset(self, bibleId, filesetId, hashId, bookIdSet, databasePath):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = ("SELECT book_id, chapter, verse_start, verse_end, verse_text FROM bible_verses"
			" WHERE hash_id = %s AND book_id IN ('") + "','".join(bookIdSet) + "')"
		resultSet = self.db.select(sql, (hashId))
		dbpVerseMap = {}
		for (dbpBookId, dbpChapter, dbpVerseStart, dbpVerseEnd, dbpVerseText) in resultSet:
			dbpVerseMap[(dbpBookId, dbpChapter, dbpVerseStart)] = (dbpVerseEnd, dbpVerseText)
		ssBookIdSet = set()
		verseList = self.getBiblePublisherVerses(filesetId, databasePath)
		for (ssBookId, ssChapter, ssVerseStart, ssVerseEnd, ssVerseText) in verseList:
			ssBookIdSet.add((ssBookId, ssChapter, ssVerseStart))

			if (ssBookId, ssChapter, ssVerseStart) not in dbpVerseMap.keys():
				ssVerseText = ssVerseText.replace("'", "\\'")
				insertRows.append((ssVerseEnd, ssVerseText, hashId, ssBookId, ssChapter, ssVerseStart))
			else:
				(dbpVerseEnd, dbpVerseText) = dbpVerseMap.get((ssBookId, ssChapter, ssVerseStart))
				if dbpVerseEnd != ssVerseEnd:
					updateRows.append(("verse_end", ssVerseEnd, dbpVerseEnd, hashId, ssBookId, ssChapter, ssVerseStart))
				if dbpVerseText != ssVerseText:
					ssVerseText = ssVerseText.replace("'", "\\'")
					shortenedPriorText = dbpVerseText.split('\n')[0][0:50]
					updateRows.append(("verse_text", ssVerseText, shortenedPriorText, hashId, ssBookId, ssChapter, ssVerseStart))

		for (dbpBookId, dbpChapter, dbpVerseStart) in dbpVerseMap.keys():
			if (dbpBookId, dbpChapter, dbpVerseStart) not in ssBookIdSet:
				deleteRows.append((hashId, dbpBookId, dbpChapter, dbpVerseStart))

		tableName = "bible_verses"
		pkeyNames = ("hash_id", "book_id", "chapter", "verse_start")
		attrNames = ("verse_end", "verse_text")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)
		self.tempDeleteTextFormat(filesetId) ## Remove when we start adding text_format with load


	def tempDeleteTextFormat(self, filesetId):
		sql = "SELECT hash_id FROM bible_filesets WHERE set_type_code = 'text_format' AND id=%s"
		hashId = self.db.selectScalar(sql, (filesetId,))
		if hashId != None:
			self.dbOut.rawStatement("DELETE FROM bible_files WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_verses WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM access_group_filesets WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_fileset_tags WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_fileset_copyright_organizations WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_fileset_copyrights WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_fileset_connections WHERE hash_id = '%s';" % (hashId,))
			self.dbOut.rawStatement("DELETE FROM bible_filesets WHERE hash_id = '%s';" % (hashId,))	


	# This method concatonates together those verses which have the same ending code.
	def getBiblePublisherVerses(self, filesetId, databasePath):
		results = []
		priorBookId = None
		priorChapter = -1
		priorVerseEnd = 0
		bibleDB = SqliteUtility(databasePath)		
		for (reference, verseText) in bibleDB.select("SELECT reference, html FROM verses", ()):
			(bookId, chapter, verseNum) = reference.split(":")
			chapterNum = int(chapter)
			if (priorBookId != bookId or priorChapter != chapterNum):
				priorBookId = bookId
				priorChapter = chapterNum
				priorVerseEnd = 0
			parts = verseNum.split("-")
			verseStart = self.verseString2Int(parts[0])
			if len(parts) > 1:
				verseEnd = self.verseString2Int(parts[1])
			else:
				verseEnd = verseStart
			verseText = verseText.replace('\r', '')

			if verseStart > priorVerseEnd:
				results.append((bookId, chapterNum, verseStart, verseEnd, verseText))
			else:
				(lastBookId, lastChapter, lastVerseStart, lastVerseEnd, lastVerseText) = results.pop()
				newVerseText = lastVerseText + '  ' + verseText
				results.append((bookId, chapterNum, lastVerseStart, verseEnd, newVerseText))
			priorVerseEnd = verseEnd
		bibleDB.close()
		return results


	def verseString2Int(self, verse):
		if verse.isdigit():
			return int(verse)
		else:
			return int(verse[0:-1])


if (__name__ == '__main__'):
	from LPTSExtractReader import *
	from InputFileset import *
	from DBPLoadController import *

	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.validate(filesets)

	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut)
	for inp in InputFileset.upload:
		hashId = update.processFileset(inp.typeCode, inp.bibleId, inp.filesetId, inp.fullPath(), inp.csvFilename, inp.databasePath)

	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-" + inp.filesetId)

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test GNWNTM

# time python3 load/UpdateDBPTextFilesets.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/UpdateDBPTextFilesets.py test /Volumes/FCBH/all-dbp-etl-test/ GNWNTMN2ET
# time python3 load/UpdateDBPTextFilesets.py test /Volumes/FCBH/all-dbp-etl-test/ text/GNWNTM/GNWNTMN2ET



