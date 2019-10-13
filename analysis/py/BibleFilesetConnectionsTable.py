# BibleFilesetConnectionsTable.py
#
# There are only two records that have the same hash_id repeated with different bible_ids
#
#| hash_id    | char(12)    | NO   | PRI | NULL              |
#| bible_id   | varchar(12) | NO   | PRI | NULL              | 
#| PRIMARY KEY (hash_id, bible_id)
#| KEY (bible_id)
#| FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id)
#| FOREIGN KEY (bible_id) REFERENCES bibles (id)
#

# Get a list of filesetId's and hashIds from the database
# Get a map of the list of biblesId associated with each filesetId in the bucket

import io
import os
import sys
from Config import *
from SQLUtility import *

class BibleFilesetConnectionsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		filesetList = self.db.select("SELECT distinct hash_id, bible_id FROM bucket_verse_summary", None)
		print("num filesets in bible_filesets %d" % (len(filesetList)))
		results = []

		for row in filesetList:
			hashId = row[0]
			bibleId = row[1]
			results.append((hashId, bibleId))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)", results)


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
connects = BibleFilesetConnectionsTable(config, db)
connects.process()
db.close()




