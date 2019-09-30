# CompareTable.py

# This program iterates over each table that is generated.
# It compares two databases.
# It should work one table at a time, and one column at a time
# It should also be possible to generate results over a wide variety of tables and columns
#
from SQLUtility import *
from LPTSExtractReader import *

class CompareTable:

	def __init__(self, config):
		self.config = config
		self.prod_db = "dbp_only"
		#self.prod_db = "dbp"
		self.test_db = "valid_dbp"
		self.db = SQLUtility("localhost", 3306, "root", self.prod_db)
		self.tables = {}
		self.tables["bibles"] = [["id"],["language_id", "versification", "numeral_system_id", "date", "scope", 
			"script", "derived", "copyright", "priority", "reviewed", "notes"]]
		self.tables["bible_filesets"] = [["hash_id"], ["id", "asset_id", "set_type_code", "hidden"]] # check set_size_code after bible_files
		self.tables["bible_files"] = [["hash_id", "book_id", "chapter_start", "verse_start"], 
			["chapter_end", "verse_end", "file_name", "file_size", "duration"]]


	def comparePkey(self, table):
		print(table.upper())
		tableDef = self.tables[table]
		pkey = tableDef[0]		
		print("pKey: %s" % (pkey))
		columns = tableDef[1]

		sqlCount = "SELECT count(*) FROM %s.%s"
		prodCount = self.db.selectScalar(sqlCount % (self.prod_db, table), None)
		testCount = self.db.selectScalar(sqlCount % (self.test_db, table), None)
		countRatio = int(round(100.0 * testCount / prodCount))
		print("table %s counts: production=%d, test=%d, ratio=%d" % (table, prodCount, testCount, countRatio))
	
		onClause = self.privateOnPhrase(pkey)
		sqlMatchCount = "SELECT count(*) FROM %s.%s p JOIN %s.%s t ON %s"
		matchCount = self.db.selectScalar(sqlMatchCount % (self.prod_db, table, self.test_db, table, onClause), None)
		sqlMismatchCount = "SELECT count(*) FROM %s.%s p WHERE NOT EXISTS (SELECT 1 FROM %s.%s t WHERE %s)"
		prodMismatchCount = self.db.selectScalar(sqlMismatchCount % (self.prod_db, table, self.test_db, table, onClause), None)
		testMismatchCount = self.db.selectScalar(sqlMismatchCount % (self.test_db, table, self.prod_db, table, onClause), None)
		print("num match = %d, prod mismatch = %d,  test mismatch = %d" % (matchCount, prodMismatchCount, testMismatchCount))
	
		selectList = ",".join(pkey) + "," + ",".join(columns)
		sqlMismatch = "SELECT %s FROM %s.%s p WHERE NOT EXISTS (SELECT 1 FROM %s.%s t WHERE %s) limit 10"
		prodMismatches = self.db.select(sqlMismatch % (selectList, self.prod_db, table, self.test_db, table, onClause), None)
		testMismatches = self.db.select(sqlMismatch % (selectList, self.test_db, table, self.prod_db, table, onClause), None)
		for prodMismatch in prodMismatches:
			print("In prod not test: ", prodMismatch)
		for testMismatch in testMismatches:
			print("In test not prod: ", testMismatch)


	def compareColumns(self, table):
		tableDef = self.tables[table]
		pkey = tableDef[0]
		columns = tableDef[1]
		fields = []
		for col in columns:
			fields.append("p.%s" % (col))
		selectList = ", ".join(fields)
		onClause = self.privateOnPhrase(pkey)
		fields = []
		for key in pkey:
			fields.append("p.%s" % (key))
		orderBy = ", ".join(fields)
		sql = "SELECT %s FROM %s.%s p JOIN %s.%s t ON %s ORDER BY %s"
		prodMatch = self.db.select(sql % (selectList, self.prod_db, table, self.test_db, table, onClause, orderBy), None)
		testMatch = self.db.select(sql % (selectList, self.test_db, table, self.prod_db, table, onClause, orderBy), None)
		print("num matches: prod: %d, test: %d" % (len(prodMatch), len(testMatch)))
		for col in range(len(columns)):
			prodEmptyCount = 0
			prodTestDiffCount = 0
			for rowIndex in range(len(prodMatch)):
				prodRow = prodMatch[rowIndex]
				testRow = testMatch[rowIndex]
				#print("compare %s  %s  %s"  % (columns[col], prodRow[col], testRow[col]))
				if prodRow[col] != testRow[col]:
					if prodRow[col] == None or prodRow[col] == '':
						prodEmptyCount += 1
					else:
						print("DIFF: %s  prod: %s  test: %s  At Prod Row: %s" % (columns[col], prodRow[col], testRow[col], prodRow))
						prodTestDiffCount += 1
			print("COUNTS: %s  prod empty: %d  different: %d" % (col, prodEmptyCount, prodTestDiffCount))


	def privateOnPhrase(self, pkey):
		onPhases = []
		for key in pkey:
			onPhases.append("p.%s=t.%s" % (key, key))
		return " AND ".join(onPhases)


	def biblesPkey(self):
		sqlMismatch = ("SELECT id FROM dbp_only.bibles WHERE id IN " +
			" (select bible_id from dbp_only.bible_fileset_connections) " +
			" AND id NOT IN (SELECT id FROM valid_dbp.bibles)")
		prodMismatches = self.db.selectList(sqlMismatch, None)
		for prodMismatch in prodMismatches:
			print("prod mismatch2: %s" % (prodMismatch))

	def filesetId(self):
		reader = LPTSExtractReader(self.config)
		audioSet = set(reader.getAudioMap().keys())
		textSet = set(reader.getTextMap().keys())
		videoSet = set(reader.getVideoMap().keys())
		allSet = audioSet.union(textSet, videoSet)
		sqlMismatch = ("SELECT id FROM dbp_only.bible_filesets WHERE id NOT IN" +
			" (select id from valid_dbp.bible_filesets)")
		prodMismatches = self.db.selectList(sqlMismatch, None)
		for prodMismatch in prodMismatches:
			if prodMismatch[:10] in allSet:
				print("Found in fileId %s" % (prodMismatch))


	def close(self):
		self.db.close()


config = Config()
compare = CompareTable(config)
compare.comparePkey("bibles")
compare.compareColumns("bibles")
#compare.biblesPkey()
#compare.comparePkey("bible_filesets")
#compare.compareColumns("bible_filesets")
#compare.filesetId()
#compare.comparePkey("bible_files")


compare.close()






