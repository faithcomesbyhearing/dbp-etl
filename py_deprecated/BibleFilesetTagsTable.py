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
#| PRIMARY KEY (hash_id, name, language_id)
#| KEY (iso)
#| KEY (language_id)
#| FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id)
#| FOREIGN KEY (language_id) REFERENCES languages (id)
#| FOREIGN KEY (iso) REFERENCES languages (iso)
#

import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *


class BibleFilesetTagsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()
		results = []

		for fileset in self.filesetList:
			filesetId = fileset[0]
			typeCode = fileset[1][0:3]
			hashId = fileset[2]
			for name in ["bitrate", "sku", "volume"]:
				description = self.description(filesetId, typeCode, name)
				if description != None:
					adminOnly = self.adminOnly()
					notes = self.notes()
					iso = self.iso()
					languageId = self.languageId()
					results.append((hashId, name, description, adminOnly, notes, iso, languageId))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_fileset_tags (hash_id, name, description, admin_only, notes, iso, language_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", results)


	def readAll(self):
		self.filesetList = self.db.select("SELECT distinct fileset_id, set_type_code, hash_id FROM bucket_verse_summary", None)
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
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
filesets = BibleFilesetTagsTable(config, db)
filesets.process()
db.close() 




