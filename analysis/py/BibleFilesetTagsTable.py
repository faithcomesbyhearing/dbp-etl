# BibleFilesetTagsTable.py
#
#
#| hash_id     | varchar(12)      | NO   | PRI | NULL              |
#| name        | varchar(191)     | NO   | PRI | NULL              |
#| description | text             | NO   |     | NULL              |
#| admin_only  | tinyint(1)       | NO   |     | NULL              |
#| notes       | text             | YES  |     | NULL              |
#| iso         | char(3)          | NO   | MUL | NULL              |
#| language_id | int(10) unsigned | NO   | PRI | 0                 |
#

import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

# I think this should be changed to running off bucket_listing not bible_filesets

class BibleFilesetTagsTable:

	def __init__(self, config):
		self.config = config


	def readAll(self):
		self.validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def description(self, filesetId, typeCode, name):
		result = None
		if typeCode == "aud" or typeCode == "app":
			record = self.audioMap.get(filesetId[:10])
		elif typeCode == "tex":
			record = self.textMap.get(filesetId)
		elif typeCode == "vid":
			record = self.videoMap.get(filesetId)
		else:
			print("ERROR: Invalid type_code %s" % (typeCode))
			sys.exit()

		if name == "bitrate":
			if typeCode == "aud":
				if filesetId[-2:] == "16":
					result = "16kbps"
				else:
					result = "64kbps"
		elif name == "sku" and record != None:
			result = record.Reg_StockNumber()
		elif name == "volume" and record != None:
			result = record.Volumne_Name()
		return result


	def adminOnly(self):
		return 0


	def notes(self):
		return None


	def iso(self):
		return "eng"


	def languageId(self):
		return 6414


config = Config()
filesets = BibleFilesetTagsTable(config) 
filesets.readAll()
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]
	for name in ["bitrate", "sku", "volume"]:
		description = filesets.description(filesetId, typeCode, name)
		if description != None:
			adminOnly = filesets.adminOnly()
			notes = filesets.notes()
			iso = filesets.iso()
			languageId = filesets.languageId()
			results.append((hashId, name, description, adminOnly, notes, iso, languageId))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_tags (hash_id, name, description, admin_only, notes, iso, language_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()



