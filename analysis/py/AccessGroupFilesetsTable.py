# AccessGroupFilesetsTable.py
#
#
#| access_group_id | int(10) unsigned | NO   | PRI | NULL              |
#| hash_id         | char(12)         | NO   | PRI | NULL              |
#
#
#|  0 |                      |
#|  1 | TEST_GROUP           |
#|  2 | PUBLIC_DOMAIN        |
#|  3 | FCBH_GENERAL         |
#|  4 | FCBH_WEB             |
#|  5 | FCBH_GENERAL_EXCLUDE |
#|  6 | DBS_GENERAL          |
#|  7 | RESTRICTED           |
#|  8 | GIDEONS_HIDE         |
#|  9 | FCBH_HUB             |
#| 10 | BIBLEIS_HIDE         |
#

import io
import os
import sys
import hashlib
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class AccessGroupFilesetsTable:

	def __init__(self, config):
		self.config = config
		self.validDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def accessGroupIds(self, filesetId, typeCode):
		if typeCode == "aud":
			record = self.audioMap[filesetId[:10]]
		elif typeCode == "tex":
			record = self.textMap[filesetId]
		elif typeCode == "vid":
			record = self.videoMap[filesetId]
		else:
			sys.exit()

		results = []
		if record.DBPAudio() == "-1":
			results.append(3) # if 3 is not right try 9
		return results


config = Config()
filesets = AccessGroupFilesetsTable(config) 
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]

	accessGroupIds = filesets.accessGroupIds(filesetId, typeCode)
	for accessGroupId in accessGroupIds:
		results.append((accessGroupId, hashId))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO access_group_filesets (access_group_id, hash_id) VALUES (%s, %s)", results)
outputDB.close()



