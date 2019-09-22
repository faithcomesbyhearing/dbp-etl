# BibleFilesetConnectionsTable.py
#
# There are only two records that have the same hash_id repeated with different bible_ids
#
#| hash_id    | char(12)    | NO   | PRI | NULL              |
#| bible_id   | varchar(12) | NO   | PRI | NULL              | 
#

import io
import os
import sys
import hashlib
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class BibleFilesetConnectionsTable:

	def __init__(self, config):
		self.config = config
		self.validDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		self.bibleIdList = self.validDB.selectList("SELECT id from bibles", None)
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def bibleIds(self, filesetId, typeCode):
		if typeCode == "aud":
			lookup = self.audioMap
		elif typeCode == "tex":
			lookup = self.textMap
		elif typeCode == "vid":
			lookup = self.videoMap
		else:
			print("ERROR: invalid typeCode %s in %s" % (typeCode, filesetId))
			sys.exit()
		results = []
		id1 = lookup[filesetId[:10]].DBP_Equivalent()
		if id1 != None:
			if id1 in self.bibleIdList:
				results.append(id1)
			else:
				print("WARNING: filesetId %s not added, because bibleId %s not in Bibles" % (filesetId, id1))
		id2 = lookup[filesetId[:10]].DBP_Equivalent2()
		if id2 != None:
			if id2 in self.bibleIdList:
				results.append(id2)
			else:
				print("WARNING: filesetId %s not added, because bibleId %s in Bibles" % (filesetId, id2))
		return results



config = Config()
filesets = BibleFilesetConnectionsTable(config) 
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]
	bibleIds = filesets.bibleIds(filesetId, typeCode)
	for bibleId in bibleIds:
		results.append((hashId, bibleId))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)", results)
outputDB.close()



