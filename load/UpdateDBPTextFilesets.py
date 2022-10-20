# UpdateDBPTextFilesets.py


import io
import sys
import os
import re
import sqlite3
from Config import *
from Log import *
from LanguageReader import *
from SQLUtility import *
from SQLBatchExec import *
from S3Utility import *
from SqliteUtility import *


class UpdateDBPTextFilesets:

	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.newFilesetId = None


	## This is called one fileset at a time by Validate.process
	def validateFileset(self, subTypeCode, bibleId, filesetId, languageRecord, lptsIndex, fullFilesetPath):
		if languageRecord == None or lptsIndex == None:
			return((Log.EROR, "has no LPTS record."))
		iso3 = languageRecord.ISO()
		if iso3 == None:
			return((Log.EROR, "has no iso language code."))
		scriptName = languageRecord.Orthography(lptsIndex)
		if scriptName == None:
			return((Log.EROR, "has no Orthography."))
		scriptId = self.db.selectScalar("SELECT script_id FROM lpts_script_codes WHERE lpts_name = %s", (scriptName,))
		if scriptId == None:
			return((Log.EROR, "%s script name is not in lpts_script_codes." % (scriptName,)))
		direction = self.db.selectScalar("SELECT direction FROM alphabets WHERE script = %s", (scriptId,))
		if direction not in {"ltr", "rtl"}:
			return((Log.EROR, "%s script has no direction in alphabets." % (scriptId,)))
		if subTypeCode == "text_format":
			# BWF. six character filesetid is deprecated. Analysis shows that calls to this method should not send subTypeCode=text_format 
			print("**** text_format subTypeCode... creating SIX character filesetId")
			#self.newFilesetId = filesetId[:6]
		elif subTypeCode == "text_plain":
			self.newFilesetId = filesetId.split("-")[0]
		elif subTypeCode == "text_html":
			self.newFilesetId = filesetId.split("-")[0] + "-html"
		elif subTypeCode == "text_json":
			self.newFilesetId = filesetId.split("-")[0] + "-json"	

		return None

	def invokeBiblePublisher(self, inp, fullFilesetPath):
		iso3 = inp.languageRecord.ISO()
		iso1 = self.db.selectScalar("SELECT iso1 FROM languages WHERE iso = %s AND iso1 IS NOT NULL", (inp.languageRecord.ISO(),))
		if iso1 == None:
			iso1 = "null"

		scriptId = self.db.selectScalar("SELECT script_id FROM lpts_script_codes WHERE lpts_name = %s", (inp.languageRecord.Orthography(inp.index),))

		direction = self.db.selectScalar("SELECT direction FROM alphabets WHERE script = %s", (scriptId,))

		# invoke Bible Publisher		
		databaseName = inp.filesetId.split("-")[0]
		cmd = [self.config.node_exe,
			self.config.publisher_js,
			fullFilesetPath,
			self.config.directory_accepted,
			databaseName, iso3, iso1, direction]
		print("================= Invoke BiblePublisher ========================")
		print("cmd: ", cmd)
		print("=========================================")
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=120)
		if response == None or response.returncode != 0:
			return((Log.EROR, "BiblePublisher error: " + str(response.stderr.decode("utf-8"))))
		print("BiblePublisher invocation successful:", str(response.stdout.decode("utf-8")))	

		return None

	def invokeSofriaCli(self, fullFilesetPath, filesetLptsName):
		# invoke sofria
		# this is the invocation of the sofria-cli. It should be similar to the invocation of BiblePublisher
		cmd = [self.config.node_exe,
			self.config.sofria_client_js,
			fullFilesetPath,
			self.config.directory_accepted,
			filesetLptsName]
		print("================= Invoke Sofria Cli ========================")
		print("cmd: ", cmd)
		print("=========================================")
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=900)
		if response == None or response.returncode != 0:
			return((Log.EROR, "Sofria error: " + str(response.stderr.decode("utf-8"))))
		print("Sofria invocation successful:", str(response.stdout.decode("utf-8")))

		return None

	def createTextFileset(self, inputFileset):
		inp = inputFileset
		textFileset = InputFileset(self.config, inp.location, self.newFilesetId, inp.filesetPath, inp.lptsDamId, inp.typeCode, inp.bibleId, inp.index, inp.languageRecord)
		inp.numberUSXFileset(textFileset) 
		return textFileset
	
	def createJSONFileset(self, inputFileset):
		inp = inputFileset
		textFileset = InputFileset(self.config, inp.location, self.newFilesetId, inp.filesetPath, inp.lptsDamId, inp.typeCode, inp.bibleId, inp.index, inp.languageRecord)
		return textFileset


	def updateFilesetTextPlain(self, bibleId, filesetId, hashId, bookIdSet, databasePath):
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
			verseNumInput = self.removeSpecialChar(verseNum)
			parts = re.split("[-,]", verseNumInput)
			verseStart = self.verseString2Int(bookId, chapter, parts[0])
			verseEnd = self.verseString2Int(bookId, chapter, parts[len(parts) -1])
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

	def removeSpecialChar(self, verse):
		listCharacters = ['\u200f', '\u200c']
		newVerse = verse
		for character in listCharacters:
			newVerse = newVerse.replace(character, '')
		return newVerse

	def verseString2Int(self, bookId, chapter, verse):
		if verse.isdigit():
			return int(verse)
		else:
			print("ERROR: Verse %s contains non-number at %s %s" % (verse, bookId, chapter))
			sys.exit()


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from LanguageReader import *
	from InputFileset import *
	from DBPLoadController import *

	config = Config.shared()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, languageReader)
	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, languageReader)
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



