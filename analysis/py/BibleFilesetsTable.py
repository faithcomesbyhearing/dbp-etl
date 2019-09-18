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
from Config import *
from LPTSExtractReader import *
from SQLUtility import *
from LookupTables import *
from BucketReader import *

class BibleFilesetsTable:

	def __init__(self, config):
		bucket = BucketReader(config)
		self.filesetIds = bucket.filesetIds()
		print("num fileset Ids", len(self.filesetIds))
		## I might not need the DB ??
		self.inputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_input_db_name)
		## I might not need the Extract ??
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in list", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in list", len(self.textMap.keys()))
		## We also need to read the video bucket here !!!!


	def filesetId(self, bible):
		result = None
		return None


	def hashId(self, bible):
		result = None
		return result


	def assetId(self, bible):
		result = None
		return result


	def setTypeCode(self, bible):
		result = None
		return result


	def setSizeCode(self, bible):
		result = None
		return result


	def hidden(self, bible):
		result = None
		return result


config = Config()
filesets = BibleFilesetsTable(config)
print("num filesets in dbp-prod", len(filesets.filesetIds))
sys.exit()
results = []
for filesetId in filesets.filesetIds:
	fileset = bibles.bibleMap.get(bibleId)
	if fileset != None:
		hashId = fileset.hashId(bible)
		assetId = fileset.assetId(bible)
		setTypeCode = fileset.setTypeCode(bible)
		setSizeCode = fileset.setSizeCode(bible)
		hidden = fileset.hidden(bible)
		results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden))
	else:
		print("WARNING LPTS has no record for %s" % (filesetId))

filesets.inputDB.close()
#outputDB = SQLUtility(config.database_host, config.database_port,
#			config.database_user, config.database_output_db_name)
#outputDB.executeBatch("INSERT INTO bible_translations (language_id, bible_id, vernacular, vernacular_trade, name, description, background, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", results)
#outputDB.close()



