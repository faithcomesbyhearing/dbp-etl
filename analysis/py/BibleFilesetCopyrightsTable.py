# BibleFilesetCopyrightsTable.py
#
#
#| hash_id               | char(12)     | NO   | PRI | NULL              |                             |
#| copyright_date        | varchar(191) | YES  |     | NULL              |                             |
#| copyright             | text         | NO   |     | NULL              |                             |
#| copyright_description | text         | NO   |     | NULL              |                             |
#| open_access           | tinyint(1)   | NO   |     | 1                 |                             |
#

import io
import os
import sys
#import hashlib
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class BibleFilesetCopyrightsTable:

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

	def copyright(self, fileset):
		result = []
		if fileset.Copyrightc() != None:
			result.append("Text: %s" % (fileset.Copyrightc()))
		if fileset.Copyrightp() != None:
			result.append("Audio: %s" % (fileset.Copyrightp()))
		if fileset.Copyright_Video() != None:
			result.append("Video: %s" % (fileset.Copyright_Video()))

		return "\n".join(result)


	def copyrightDate(self, copyright):		
		parts = copyright.split(" ")
		result = None
		for part in parts:
			if len(part) == 4 and self.privateIsInt(part):
				result = part
		return result

	def privateIsInt(self, value):
		try: 
			int(value)
			return True
		except ValueError:
			return False


	def copyrightDescription(self, copyright):
		# use the copyright value 
		return copyright

	def openAccess(self):
		# This defaults to 1, which I think is not correct.
		return 1



config = Config()
filesets = BibleFilesetCopyrightsTable(config) 
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]
	if typeCode == "aud":
		lookup = filesets.audioMap[filesetId[:10]]
	elif typeCode == "tex":
		lookup = filesets.textMap[filesetId]
	elif typeCode == "vid":
		lookup = filesets.videoMap[filesetId]
	else:
		print("ERROR: fileset %s has unknown type %s" % (filesetId, typeCode))
		sys.exit()

	copyright = filesets.copyright(lookup)
	copyrightDate = filesets.copyrightDate(copyright)
	copyrightDescription = filesets.copyrightDescription(copyright)
	openAccess = filesets.openAccess()
	results.append((hashId, copyrightDate, copyright, copyrightDescription, openAccess))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_copyrights (hash_id, copyright_date, copyright, copyright_description, open_access) VALUES (%s, %s, %s, %s, %s)", results)
outputDB.close()



