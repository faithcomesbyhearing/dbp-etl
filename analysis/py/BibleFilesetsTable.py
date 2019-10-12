# BibleFilesetsTable.py
#
#
#| id            | varchar(16) | NO   | MUL | NULL              |                             |
#| hash_id       | char(12)    | NO   | PRI | NULL              |                             |
#| asset_id      | varchar(64) | NO   | MUL |                   |                             |
#| set_type_code | varchar(16) | NO   | MUL | NULL              |                             |
#| set_size_code | char(9)     | NO   | MUL | NULL              |                             |
#| hidden        | tinyint(1)  | NO   |     | 0                 |                             |

import io
import os
import sys
import hashlib
from Config import *
from SQLUtility import *

# legacy_asset_id was added here temporarily, and ideally should be removed for deployment
# it was needed in order that hash_id can match between this system and dbp. GNG 10/9/19

class BibleFilesetsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.filesets = self.db.select("SELECT distinct asset_id, fileset_id, set_type_code, hash_id, legacy_asset_id FROM bucket_verse_summary", None)
		print("num filesets %d" % (len(self.filesets)))
		results = []
		for fileset in bible.filesets:
			assetId = fileset[0] 
			filesetId = fileset[1]
			setTypeCode = fileset[2]
			hashId = fileset[3]
			legacyAssetId = fileset[4]
			setSizeCode = "S" # initial setting updated later
			hidden = "0"
			results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden, legacyAssetId))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_filesets (id, hash_id, asset_id, set_type_code, set_size_code, hidden, legacy_asset_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", results)


config = Config()
db = SQLUtility(config.database_host, config.database_port,
		config.database_user, config.database_output_db_name)
bible = BibleFilesetsTable(config, db)
bible.process()
db.close()



