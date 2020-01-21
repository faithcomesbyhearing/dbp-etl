# UpdateDBPVerseTable.py

# This class is only used in the full load of a database
# if the original source of the verse data is unavailable.
# It takes the verse data from the current database.

import io
import os
import sys
from Config import *
from SQLUtility import *
from UpdateDBPDatabase import *


SOURCE_DATABASE = "dbp"
TARGET_ASSET_ID = "db"


class UpdateDBPVerseTable:

	def __init__(self, config):
		self.db = SQLUtility(config.database_host, config.database_port, config.database_user, config.database_db_name)
		self.OT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'OT'", ())
		self.NT = self.db.selectSet("SELECT id FROM books WHERE book_testament = 'NT'", ())
		self.statements = []


	def loadVerseTable(self):
		sql = "SELECT id, hash_id FROM %s.bible_filesets WHERE set_type_code = 'text_plain'" % (SOURCE_DATABASE)
		filesetIdMap = self.db.selectMap(sql, ())
		print(len(filesetIdMap.keys()), "verse filesets found")
		for filesetId in sorted(filesetIdMap.keys()):
			hashId = filesetIdMap[filesetId]
			print(filesetId)
			self.statements = []

			sql = "SELECT distinct book_id FROM " + SOURCE_DATABASE + ".bible_verses WHERE hash_id = %s"
			bookIdList = self.db.selectSet(sql, (hashId,))
			if len(bookIdList) == 0:
				print("WARNING: plain_text filesetId %s has no verses." % (filesetId))
			else:
				otBooks = bookIdList.intersection(self.OT)
				ntBooks = bookIdList.intersection(self.NT)
				setSizeCode = UpdateDBPDatabase.getSetSizeCode(ntBooks, otBooks)
				bucket = TARGET_ASSET_ID
				setTypeCode = 'text_plain'
				newHashId = UpdateDBPDatabase.getHashId(bucket, filesetId, setTypeCode)
				sql = ("INSERT INTO bible_filesets(id, hash_id, asset_id, set_type_code,"
					" set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, 0)")
				values = (filesetId, newHashId, bucket, setTypeCode, setSizeCode)
				self.statements.append((sql, [values]))

				sql = ("SELECT book_id, chapter, verse_start, verse_end, verse_text FROM "
					+ SOURCE_DATABASE + ".bible_verses WHERE hash_id = %s")
				resultSet = self.db.select(sql, (hashId,))
				values = []
				for row in resultSet:
					values.append((newHashId,) + row)
				sql = ("INSERT INTO bible_verses (hash_id, book_id, chapter, verse_start, verse_end, verse_text)"
					" VALUES (%s, %s, %s, %s, %s, %s)")
				self.statements.append((sql, values))
				self.db.executeTransaction(self.statements)


	def dropIndex(self):
		sql = "ALTER TABLE bible_verses DROP INDEX verse_text"
		self.db.execute(sql, ())

	def addIndex(self):
		sql = "ALTER TABLE bible_verses ADD FULLTEXT INDEX verse_text (verse_text)"
		self.db.execute(sql, ())


config = Config("dev")
verses = UpdateDBPVerseTable(config)
verses.dropIndex()
verses.loadVerseTable()
#verses.addIndex()

				
