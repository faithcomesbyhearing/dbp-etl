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
from VersesReader import *

# legacy_asset_id was added here temporarily, and ideally should be removed for deployment
# it was needed in order that hash_id can match between this system and dbp. GNG 10/9/19

class BibleFilesetsTable:

	def __init__(self, config):
		self.config = config


	def readAll(self):
		db = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesets = db.select("SELECT distinct bucket, fileset_id, set_type_code, hash_id, legacy_asset_id FROM bucket_listing", None)
		db.close()
		print("num filesets %d" % (len(self.filesets)))
		verseReader = VersesReader(self.config)

		## The following is a temporary solution for handling verses.  I need to study output
		self.verseFilesets = verseReader.filesetIds()
		print("num verse filesets %d" % (len(self.verseFilesets)))
		sqlUtility = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_input_db_name)
		self.legacyFilesetMap = sqlUtility.selectMapList("SELECT concat(id, set_type_code), asset_id FROM bible_filesets", None)
		sqlUtility.close()
		print("num fileset/set_type_code in dbp %d" % (len(self.legacyFilesetMap.keys())))


	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def legacyAssetId(self, filesetId, setTypeCode, assetId):
		key = filesetId + setTypeCode
		legacyIds = self.legacyFilesetMap.get(key)
		if legacyIds == None:
			print("no legacy bucket found actual one returned")
			return assetId
		elif len(legacyIds) == 1:
			return legacyIds[0]
		else:
			print("multiple Ids %s  %s  %s" % (filesetId, setTypeCode, ",".join(legacyIds)))
			# When there are two the first is always dbp-prod, the second is dbp-dbs
			return legacyIds[0]



config = Config()
bible = BibleFilesetsTable(config) 
bible.readAll()
results = []

for fileset in bible.filesets:
	bucket = fileset[0] 
	filesetId = fileset[1]
	setTypeCode = fileset[2]
	hashId = fileset[3]
	legacyAssetId = fileset[4]

	setSizeCode = "S" # initial setting updated later
	hidden = "0"
	results.append((filesetId, hashId, bucket, setTypeCode, setSizeCode, hidden, legacyAssetId))

for filesetId in bible.verseFilesets:
	assetId = "db" # Should this be in config.xml somewhere?
	setTypeCode = "text_plain"
	legacyAssetId = bible.legacyAssetId(filesetId, setTypeCode, assetId)
	hashId = bible.hashId(legacyAssetId, filesetId, setTypeCode)
	setSizeCode = "S" # initial setting
	hidden = "0"
	results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden, legacyAssetId))

print("num records to insert %d" % (len(results)))
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_filesets (id, hash_id, asset_id, set_type_code, set_size_code, hidden, legacy_asset_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()



