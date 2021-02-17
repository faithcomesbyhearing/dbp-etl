# UpdateDBPBibleTranslations

from Config import *
from SQLUtility import *
from SQLBatchExec import *
from LPTSExtractReader import *


class UpdateDBPBibleTranslations:


	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader


	def insertEngVolumeName(self):
		insertRows = []
		updateRows = []
		deleteRows = []
		engLanguageId = 6414

		dbpMap = {}
		sql = "SELECT bible_id, name, vernacular FROM bible_translations WHERE language_id = %s"
		resultSet = self.db.select(sql, (engLanguageId,))
		for (dbpBibleId, dbpName, dbpVernacular)in resultSet:
			dbpMap[dbpBibleId] = (dbpName, dbpVernacular)

		## retrieve bibles from LPTS
		lptsBibleMap = self.lptsReader.getBibleIdMap()
		lptsBibleMap.pop("JESUS FILM", None) # delete JESUS FILM

		for bibleId in sorted(lptsBibleMap.keys()):
			lptsRecords = lptsBibleMap[bibleId]
			volumeName = self.biblesVolumneName(bibleId, lptsRecords)

			dbpNameCols = dbpMap.get(bibleId)

			if dbpNameCols == None and volumeName != None:
				insertRows.append((volumeName.replace("'", "\\'"), 0, bibleId, engLanguageId))
			elif dbpNameCols != None and volumeName == None:
				deleteRows.append((bibleId, engLanguageId))
			elif dbpNameCols != None and volumeName != None:
				(dbpName, dbpVernacular) = dbpNameCols
				if dbpName != volumeName:
					updateRows.append(("name", volumeName.replace("'", "\\'"), dbpName.split("\n")[0], bibleId, engLanguageId))

		tableName = "bible_translations"
		pkeyNames = ("bible_id", "language_id")
		attrNames = ("name", "vernacular")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def biblesVolumneName(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			volName = lptsRecord.Volumne_Name()
			if volName != None:
				final.add(volName)
		if len(final) == 0:
			return None
		elif len(final) > 1:
			#print("WARN: bible_id %s has multiple volumne_name |%s|" % (bibleId, "|".join(final)))
			return max(final, key=len)
		return list(final)[0]


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	bibles = UpdateDBPBibleTranslations(config, db, dbOut, lptsReader)
	bibles.insertEngVolumeName()
	db.close()

	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute("test-bibles")


