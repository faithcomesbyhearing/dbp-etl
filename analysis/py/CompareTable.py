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
		tableDef = self.tables[table]
		#if len(tableDef[0]) == 1:
		#	pkey = tableDef[0][0]
		#else:
		#	#pkey = "concat(" + ",".join(tableDef[0]) + ")"
		#	p
		pkey = tableDef[0][0]
		print("pKey: %s" % (pkey))

		sqlCount = "SELECT count(*) FROM %s.%s"
		prodCount = self.db.selectScalar(sqlCount % (self.prod_db, table), None)
		testCount = self.db.selectScalar(sqlCount % (self.test_db, table), None)
		countRatio = int(round(100.0 * testCount / prodCount))
		print("table %s counts: production=%d, test=%d, ratio=%d" % (table, prodCount, testCount, countRatio))
	
		sqlMatchCount = "SELECT count(distinct %s) FROM %s.%s p WHERE %s IN (SELECT %s FROM %s.%s)"
		matchCount = self.db.selectScalar(sqlMatchCount % (pkey, self.prod_db, table, pkey, pkey, self.test_db, table), None)
		sqlMismatchCount = "SELECT count(distinct %s) FROM %s.%s WHERE %s NOT IN (SELECT %s FROM %s.%s)"
		prodMismatchCount = self.db.selectScalar(sqlMismatchCount % (pkey, self.prod_db, table, pkey, pkey, self.test_db, table), None)
		testMismatchCount = self.db.selectScalar(sqlMismatchCount % (pkey, self.test_db, table, pkey, pkey, self.prod_db, table), None)
		print("num match = %d, prod mismatch = %d,  test mismatch = %d" % (matchCount, prodMismatchCount, testMismatchCount))
	
		sqlMismatch = "SELECT * FROM %s.%s WHERE %s NOT IN (SELECT %s FROM %s.%s) limit 10"
		prodMismatches = self.db.select(sqlMismatch % (self.prod_db, table, pkey, pkey, self.test_db, table), None)
		testMismatches = self.db.select(sqlMismatch % (self.test_db, table, pkey, pkey, self.prod_db, table), None)
		for prodMismatch in prodMismatches:
			print("prod mismatch: ", prodMismatch)
		for testMismatch in testMismatches:
			print("test mismatch: ", testMismatch)


	def compareColumns(self, table):
		tableDef = self.tables[table]
		pkey = tableDef[0][0]
		columns = tableDef[1]
		colStr = ",".join(columns)
		sql = "SELECT %s FROM %s.%s WHERE %s IN (SELECT %s FROM %s.%s)"
		prodMatch = self.db.select(sql % (colStr, self.prod_db, table, pkey, pkey, self.test_db, table), None)
		testMatch = self.db.select(sql % (colStr, self.test_db, table, pkey, pkey, self.prod_db, table), None)
		print("num matches: prod: %d, test: %d" % (len(prodMatch), len(testMatch)))
		for rowIndex in range(len(prodMatch)):
			prodRow = prodMatch[rowIndex]
			testRow = testMatch[rowIndex]
			for col in range(len(columns)):
				#print("compare %s  %s  %s"  % (columns[col], prodRow[col], testRow[col]))
				if prodRow[col] != testRow[col]:
					print("DIFF: %s  prod: %s  test: %s  At Row: %s" % (columns[col], prodRow[col], testRow[col], testRow))



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
#compare.comparePkey("bibles")
#compare.biblesPkey()
compare.comparePkey("bible_filesets")
#compare.compareColumns("bible_filesets")
#compare.filesetId()
#compare.comparePkey("bible_files")

compare.close()




"""
Rewrite the comparePkey using this syntax
SELECT a.x, a.y FROM a JOIN b ON a.x = b.x AND a.y = b.y;
SELECT p.hash_id FROM dbp p JOIN valid_dbp t ON p.hash_id = t.hash_id
Except:

SELECT a.* FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.x = a.x)

match 1 pKey
SELECT p.hash_id FROM dbp.bible_filesets p JOIN valid_dbp.bible_filesets t ON p.hash_id = t.hash_id;
count 1 pkey on match
SELECT count( distinct p.hash_id) FROM dbp.bible_filesets p JOIN valid_dbp.bible_filesets t ON p.hash_id = t.hash_id;

SELECT p.hash_id, p.book_id, p.chapter_start, p.verse_start FROM dbp.bible_files p JOIN valid_dbp.bible_files t ON p.hash_id=t.hash_id AND p.book_id=t.book_id AND p.chapter_start=t.chapter_start AND p.verse_start=t.verse_start;
match many

SELECT count(distinct p.hash_id, p.book_id, p.chapter_start, p.verse_start) FROM dbp.bible_files p JOIN valid_dbp.bible_files t ON p.hash_id=t.hash_id AND p.book_id=t.book_id AND p.chapter_start=t.chapter_start AND p.verse_start=t.verse_start;
match count

SELECT count(*) FROM dbp.bible_files p JOIN valid_dbp.bible_files t ON p.hash_id=t.hash_id AND p.book_id=t.book_id AND p.chapter_start=t.chapter_start AND p.verse_start=t.verse_start;
match count with count(*)

SELECT p.hash_id FROM dbp_only.bible_filesets p WHERE NOT EXISTS (SELECT 1 FROM valid_dbp.bible_filesets t WHERE p.hash_id = t.hash_id);

SELECT p.hash_id, p.book_id, p.chapter_start, p.verse_start FROM dbp_only.bible_files p WHERE NOT EXISTS (SELECT 1 FROM valid_dbp.bible_files t WHERE p.hash_id=t.hash_id AND p.book_id=t.book_id AND p.chapter_start=t.chapter_start AND p.verse_start=t.verse_start );
difference with multi columns
"""






