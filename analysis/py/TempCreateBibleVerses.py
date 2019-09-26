#TempCreateBibleVerses.py
#
# This is a temporary program for preliminary testing.  It reads the production tables
# bible_verses, bible_filesets, and bible_fileset_connections to create an input file
# for bible_verses.  So, that this file can be used for later testing.
#

import os
import io
import sys
from SQLUtility import *

class TempCreateBibleVerses:

	def process(self):
		db = SQLUtility("localhost", 3306, "root", "dbp")
		sql = ("SELECT c.bible_id, f.id, v.book_id, v.chapter, v.verse_start, v.verse_end" +
			" FROM bible_verses v, bible_filesets f, bible_fileset_connections c" +
			" WHERE v.hash_id = f.hash_id" +
			" AND v.hash_id = c.hash_id" +
			" AND f.hash_id = c.hash_id" +
			" AND f.asset_id = 'dbp-prod'")
		resultSet = db.select(sql, None)
		db.close()
		print("num verses %d" % (len(resultSet)))
		out = io.open("bible_verses.text", mode="w", encoding="utf-8")
		for row in resultSet:
			out.write("verse/%s/%s/%s_%s_%s_%s.text\n" % (row[0], row[1], row[2], row[3], row[4], row[5]))
		out.close()

temp = TempCreateBibleVerses()
temp.process()