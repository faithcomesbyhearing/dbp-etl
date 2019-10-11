# BibleFilesetConnectionsTable.py
#
# There are only two records that have the same hash_id repeated with different bible_ids
#
#| hash_id    | char(12)    | NO   | PRI | NULL              |
#| bible_id   | varchar(12) | NO   | PRI | NULL              | 
#

# Get a list of filesetId's and hashIds from the database
# Get a map of the list of biblesId associated with each filesetId in the bucket

import io
import os
import sys
from Config import *
from SQLUtility import *
from VersesReader import *

class BibleFilesetConnectionsTable:

	def __init__(self, config):
		self.config = config

	def readAll(self):
		validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesetList = validDB.select("SELECT distinct hash_id, bible_id FROM bucket_listing", None)
		print("num filesets in bible_filesets %d" % (len(self.filesetList)))
		self.verseHashIds = validDB.selectMap("SELECT id, hash_id FROM bible_filesets WHERE set_type_code='text_plain'", None)
		validDB.close()
		verses = VersesReader(config)
		self.verseBiblesFileset = verses.bibleIdFilesetId()
		print("num verse filesets in verses %d" % (len(self.verseBiblesFileset)))


config = Config()
connects = BibleFilesetConnectionsTable(config)
connects.readAll()
results = []

for row in connects.filesetList:
	hashId = row[0]
	bibleId = row[1]
	results.append((hashId, bibleId))

for row in connects.verseBiblesFileset:
	parts = row.split("/")
	bibleId = parts[0]
	filesetId = parts[1]
	hashId = connects.verseHashIds[filesetId]
	results.append((hashId, bibleId))

print("num records to insert %d" % (len(results)))

outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)", results)
outputDB.close()



