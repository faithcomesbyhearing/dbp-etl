# UpdateDBPTextFilesets.py

import sys
import os
import re
import json
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
	# def validateFileset(self, subTypeCode, bibleId, filesetId, languageRecord, lptsIndex, fullFilesetPath):
	def validateFileset(self, subTypeCode, filesetId, languageRecord, lptsIndex):
		if languageRecord == None or lptsIndex == None:
			return((Log.EROR, "has no LPTS record."))
		iso3 = languageRecord.ISO()
		if iso3 == None:
			return((Log.EROR, "has no iso language code."))
		## Orthography validation is not longer necessary
		# scriptName = languageRecord.Orthography(lptsIndex)
		# if scriptName == None:
		# 	return((Log.EROR, "has no Orthography."))
		# scriptId = self.db.selectScalar("SELECT script_id FROM lpts_script_codes WHERE lpts_name = %s", (scriptName,))
		# if scriptId == None:
		# 	return((Log.EROR, "%s script name is not in lpts_script_codes." % (scriptName,)))
		# direction = self.db.selectScalar("SELECT direction FROM alphabets WHERE script = %s", (scriptId,))
		# if direction not in {"ltr", "rtl"}:
		# 	return((Log.EROR, "%s script has no direction in alphabets." % (scriptId,)))
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
			self.newFilesetPath = self.newFilesetId
			self.newLocation = self.config.directory_accepted
		elif subTypeCode == "text_usx":
			self.newFilesetId = filesetId.split("-")[0] + "-usx"

		return None

	def invokeSofriaCli(self, fullFilesetPath, inputFilesetDBPath, inputFilesetId):
		# invoke sofria
		#
		# This is the invocation of the sofria-cli.
		#
		# This client will create and populate the tables: tableContents and verses
		#
		# The tableContents table will store data related to each book and the fields are:
		# tableContents(code, heading, title, name, chapters)
		#
		# The verses table will store data related to each verse for each book and the fields are:
		# verses(reference, text)
		#
		# The reference field is a string with the following format: "BOOK_CODE:CHAPTER_NUMBER:VERSE_RANGE" and The text field is verse content.
		cmd = [self.config.node_exe,
			self.config.sofria_client_js,
			"run",
			fullFilesetPath,
			"--populate-db=%s" % (inputFilesetDBPath),
			"--generate-json=%s" % (os.path.join(self.config.directory_accepted, inputFilesetId)),
			"--missing-verses-allowed=%s" % json.dumps(self.config.data_missing_verses_allowed).replace('\\', '')]
		print("================= Invoke Sofria Cli ========================")
		print("cmd: ", cmd)
		print("============================================================")
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=900)
		if response == None or response.returncode != 0:
			return((Log.EROR, "Sofria error.. code: %d, stdout: [%s], stderr: [%s] " % (response.returncode, str(response.stdout.decode("utf-8")), str(response.stderr.decode("utf-8")))))
		print("Sofria invocation successful:", str(response.stdout.decode("utf-8")))

		return None

	def createTextFileset(self, inputFileset):
		inp = inputFileset
		textFileset = InputFileset(self.config, inp.location, self.newFilesetId, inp.filesetPath, inp.lptsDamId, inp.typeCode, inp.bibleId, inp.index, inp.languageRecord)
		inp.numberUSXFileset(textFileset) 
		return textFileset
	
	def createJSONFileset(self, inputFileset):
		inp = inputFileset
		textFileset = InputFileset(self.config, self.newLocation, self.newFilesetId, self.newFilesetPath, inp.lptsDamId, inp.typeCode, inp.bibleId, inp.index, inp.languageRecord)
		return textFileset


	def updateFilesetTextPlain(self, bibleId, filesetId, hashId, bookIdSet, databasePath):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = ("SELECT book_id, chapter, verse_start, verse_end, verse_sequence, verse_text FROM bible_verses"
			" WHERE hash_id = %s AND book_id IN ('") + "','".join(bookIdSet) + "')"
		resultSet = self.db.select(sql, (hashId))
		dbpVerseMap = {}
		for (dbpBookId, dbpChapter, dbpVerseStart, dbpVerseEnd, dbpVerseSequence, dbpVerseText) in resultSet:
			dbpVerseMap[(dbpBookId, dbpChapter, dbpVerseStart)] = (dbpVerseEnd, dbpVerseText, dbpVerseSequence)
		ssBookIdSet = set()
		verseList = self.getBibleVersesFromSqlite(filesetId, databasePath)
		for (ssBookId, ssChapter, ssVerseStart, ssVerseEnd, ssVerseSequence, ssVerseText) in verseList:
			ssBookIdSet.add((ssBookId, ssChapter, ssVerseStart))

			if (ssBookId, ssChapter, ssVerseStart) not in dbpVerseMap.keys():
				ssVerseText = ssVerseText.replace("'", "\\'")
				insertRows.append((ssVerseEnd, ssVerseText, ssVerseSequence, hashId, ssBookId, ssChapter, ssVerseStart))
			else:
				(dbpVerseEnd, dbpVerseText, dbpVerseSequence) = dbpVerseMap.get((ssBookId, ssChapter, ssVerseStart))
				if dbpVerseEnd != ssVerseEnd:
					updateRows.append(("verse_end", ssVerseEnd, dbpVerseEnd, hashId, ssBookId, ssChapter, ssVerseStart))
				if dbpVerseText != ssVerseText:
					ssVerseText = ssVerseText.replace("'", "\\'")
					shortenedPriorText = dbpVerseText.split('\n')[0][0:50]
					updateRows.append(("verse_text", ssVerseText, shortenedPriorText, hashId, ssBookId, ssChapter, ssVerseStart))
				if dbpVerseSequence != ssVerseSequence:
					updateRows.append(("verse_sequence", ssVerseSequence, dbpVerseSequence, hashId, ssBookId, ssChapter, ssVerseStart))

		for (dbpBookId, dbpChapter, dbpVerseStart) in dbpVerseMap.keys():
			if (dbpBookId, dbpChapter, dbpVerseStart) not in ssBookIdSet:
				deleteRows.append((hashId, dbpBookId, dbpChapter, dbpVerseStart))

		tableName = "bible_verses"
		pkeyNames = ("hash_id", "book_id", "chapter", "verse_start")
		attrNames = ("verse_end", "verse_text", "verse_sequence")
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
	def getBibleVersesFromSqlite(self, filesetId, databasePath):
		results = []
		priorBookId = None
		priorChapter = -1
		priorVerseEnd = 0
		bibleDB = SqliteUtility(databasePath)
		# The `verses` table will store data related to each verse for each book and it has been populated by sofria-client.
		for (reference, verseSequence, verseText) in bibleDB.select("SELECT reference, verse_sequence, text FROM verses", ()):
			referenceArray = reference.split(":")
			if len(referenceArray) < 3:
				print("ERROR: Reference value: %s does not contain correct format" % reference)
				sys.exit()

			bookId = referenceArray[0]
			chapter = referenceArray[1]
			verseNum = referenceArray[2]

			chapterNum = int(chapter)
			if (priorBookId != bookId or priorChapter != chapterNum):
				priorBookId = bookId
				priorChapter = chapterNum
				priorVerseEnd = 0

			parts = re.split("[-,]", verseNum)

			verseStart = parts[0]
			verseEndNumber = self.verseString2Int(bookId, chapter, parts[len(parts) -1])
			verseEnd = parts[len(parts) -1]
			verseText = verseText.replace('\r', '')

			if verseSequence > priorVerseEnd:
				results.append((bookId, chapterNum, verseStart, verseEnd, verseSequence, verseText))
			else:
				(lastBookId, lastChapter, lastVerseStart, lastVerseEnd, lastVerseSequence, lastVerseText) = results.pop()
				newVerseText = lastVerseText + '  ' + verseText
				results.append((bookId, chapterNum, lastVerseStart, verseEnd, lastVerseSequence, newVerseText))
			priorVerseEnd = verseEndNumber
		bibleDB.close()
		return results

	def verseString2Int(self, bookId, chapter, verse):
		if verse.isdigit():
			return int(verse)
		else:
			verseNum = re.sub(r'[a-zA-Z]', '', verse)
			try:
				return int(verseNum)
			except:
				print("ERROR: Verse %s does not have correct format at %s %s" % (verse, bookId, chapter))
				sys.exit()


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from InputFileset import InputFileset
	from DBPLoadController import DBPLoadController
	from InputProcessor import InputProcessor
	from UpdateDBPFilesetTables import UpdateDBPFilesetTables

	config = Config.shared()
	# languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B")
	lpts_xml = config.filename_lpts_xml if migration_stage == "B" else ""
	languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)

	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)

	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, languageReader)
	ctrl.validate(filesets)

	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut, languageReader)
	for inp in InputFileset.upload:
		hashId = update.processFileset(inp)

	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-" + inp.filesetId)

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test GNWNTM

# time python3 load/UpdateDBPTextFilesets.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/UpdateDBPTextFilesets.py test /Volumes/FCBH/all-dbp-etl-test/ GNWNTMN2ET
# time python3 load/UpdateDBPTextFilesets.py test /Volumes/FCBH/all-dbp-etl-test/ text/GNWNTM/GNWNTMN2ET



