# CompareTable.py

# This program iterates over each table that is generated.
# It compares two databases.
# It should work one table at a time, and one column at a time
# It should also be possible to generate results over a wide variety of tables and columns
#
from SQLUtility import *

class CompareTable:

	def __init__(self):
		self.prod_db = "dbp_only"
		self.test_db = "valid_dbp"
		self.db = SQLUtility("localhost", 3306, "root", self.prod_db)
		self.tables = {}
		self.tables["bibles"] = [["id"],["language_id", "versification", "numeral_system_id", "date", "scope", 
			"script", "derived", "copyright", "priority", "reviewed", "notes"]]
		self.tables["bible_filesets"] = [["hash_id"], []]

	def comparePkey(self, table):
		tableDef = self.tables[table]
		pkey = tableDef[0][0] # this needs to be fixed for concatenated key
		sqlCount = "SELECT count(*) FROM %s.%s"
		prodCount = self.db.selectScalar(sqlCount % (self.prod_db, table), None)
		testCount = self.db.selectScalar(sqlCount % (self.test_db, table), None)
		countRatio = int(round(100.0 * testCount / prodCount))
		print("table %s counts: production=%d, test=%d, ratio=%d" % (table, prodCount, testCount, countRatio))
		sqlMatchCount = "SELECT count(distinct %s) FROM %s.%s WHERE %s IN (SELECT %s FROM %s.%s)"
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

	def biblesPkey(self):
		sqlMismatch = ("SELECT id FROM dbp_only.bibles WHERE id IN " +
			" (select bible_id from dbp_only.bible_fileset_connections) " +
			" AND id NOT IN (SELECT id FROM valid_dbp.bibles)")
		prodMismatches = self.db.selectList(sqlMismatch, None)
		for prodMismatch in prodMismatches:
			print("prod mismatch2: %s" % (prodMismatch))




	def close(self):
		self.db.close()


compare = CompareTable()
#compare.comparePkey("bibles")
#compare.biblesPkey()
compare.comparePkey("bible_filesets")
compare.close()






"""

	The way that I compare the primary keys of tables might need to vary from one
	table to the next, but the way that I compare nonprimary key columns might be
	consistent logic.

	bibles - to compare primary key fields, do a select .. not in query in both directions
		display the results sorted

		report on the number of differences in both directions
		show up to 10 differences when they exist

		when there is a single column primary key, all of the above can be one function

		the remaining columns can be selected one row at a time.  They should be compared with
		matching pkey values.  The percentage that match should be displayed.
		up to 10 mismatches should be displayed.

	bible_translations
		this one is troublesome, because I don't know what the primary key is.
		what if bible_id, language_id, vernacular, name all seem to have an impact
		try combining the 4 fields into a key and doing a not in with a concatenated value

	bible_filesets
		try concatenating the three fields that make up the primary key and doing a 
		not in compare in both directions
"""








