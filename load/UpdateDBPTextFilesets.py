# UpdateDBPTextFilesets.py

# Processing data in the following format
# text/{bibleId}/{filesetId}/options.xini
# text/{bibleId}/{filesetId}/{book}.usx (all the usx files to process)

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

	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader


	## This is called one fileset at a time by Validate.process
	def validateFileset(self, bibleId, filesetId):
		(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose("text", bibleId, filesetId)
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
			self.config.directory_validate + "text/%s/%s/" % (bibleId, filesetId),
			self.config.directory_accepted,
			filesetId, iso3, iso1, direction]
		response = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=120)
		if response == None or response.returncode != 0:
			return((Log.EROR, str(response.stdout.decode("utf-8"))))
		#print("text/%s/%s %s\tINFO" % (bibleId, filesetId, str(response.stdout.decode("utf-8"))))
		print("OUTPUT", str(response.stdout.decode("utf-8")))	
		return None


	def updateFileset(self, bibleId, filesetId, hashId):
		bibleDB = SqliteUtility(self.config.directory_accepted + filesetId + ".db")
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = "SELECT book_id, chapter, verse_start, verse_end, verse_text FROM bible_verses WHERE hash_id = %s"
		resultSet = self.db.select(sql, (hashId,))
		dbpVerseMap = {}
		for (dbpBookId, dbpChapter, dbpVerseStart, dbpVerseEnd, dbpVerseText) in resultSet:
			dbpVerseMap[(dbpBookId, str(dbpChapter), str(dbpVerseStart))] = (str(dbpVerseEnd), dbpVerseText)

		ssBookIdSet = set()
		for (ssReference, ssVerseText) in bibleDB.select("SELECT reference, html FROM verses", ()):
			(ssBookId, ssChapter, ssVerse) = ssReference.split(":")
			parts = ssVerse.split("-")
			if len(parts) > 1:
				ssVerseStart = parts[0]
				ssVerseEnd = parts[1]
			else:
				ssVerseStart = ssVerse
				ssVerseEnd = ssVerse
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
					updateRows.append(("verse_text", ssVerseText, dbpVerseText, hashId, ssBookId, ssChapter, ssVerseStart))

		for (dbpBookId, dbpChapter, dbpVerseStart) in dbpVerseMap.keys():
			if (dbpBookId, dbpChapter, dbpVerseStart) not in ssBookIdSet:
				deleteRows.append((hashId, dbpBookId, dbpChapter, dbpVerseStart))

		tableName = "bible_verses"
		pkeyNames = ("hash_id", "book_id", "chapter", "verse_start")
		attrNames = ("verse_end", "verse_text")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)
		bibleDB.close()


if (__name__ == '__main__'):
	from DBPLoadController import *
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)

	main = DBPLoadController(config, db, lptsReader)
	main.cleanup() # Only part of controller used here

	texts = UpdateDBPTextFilesets(config, db, dbOut, lptsReader)
	s3 = S3Utility(config)
	dirname = config.directory_validate
	for typeCode in os.listdir(dirname):
		if typeCode == "text":
			for bibleId in os.listdir(dirname + typeCode):
				for filesetId in os.listdir(dirname + typeCode + os.sep + bibleId):
					error = texts.validateFileset(bibleId, filesetId)
					if error == None:
						s3.promoteFileset(config.directory_validate, "text/%s/%s" % (bibleId, filesetId))
						texts.uploadFileset(bibleId, filesetId)
					else:
						print(error)

	filesets = UpdateDBPFilesetTables(config, db, dbOut)
	dirname = config.directory_database
	for typeCode in os.listdir(dirname):
		if typeCode == "text":
			for bibleId in os.listdir(dirname + typeCode):
				for filesetId in os.listdir(dirname + typeCode + os.sep + bibleId):
					databasePath = config.directory_accepted + filesetId + ".db"
					hashId = filesets.insertBibleFileset("verses", filesetId, databasePath)
					filesets.insertFilesetConnections(hashId, bibleId)
					texts.updateFileset(bibleId, filesetId, hashId)

	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("verses-test")
