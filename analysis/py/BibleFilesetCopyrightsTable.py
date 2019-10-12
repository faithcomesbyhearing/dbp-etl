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
from Config import *
from LPTSExtractReader import *
from SQLUtility import *


class BibleFilesetCopyrightsTable:

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
			if typeCode == "aud" or typeCode == "app":
				lookup = self.audioMap.get(filesetId[:10])
			elif typeCode == "tex":
				lookup = self.textMap.get(filesetId)
			elif typeCode == "vid":
				lookup = self.videoMap.get(filesetId)
			else:
				print("ERROR: fileset %s has unknown type %s" % (filesetId, typeCode))
				sys.exit()

			copyright = self.copyright(lookup)
			copyrightDate = self.copyrightDate(copyright)
			copyrightDescription = self.copyrightDescription(copyright)
			openAccess = self.openAccess()
			results.append((hashId, copyrightDate, copyright, copyrightDescription, openAccess))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_fileset_copyrights (hash_id, copyright_date, copyright, copyright_description, open_access) VALUES (%s, %s, %s, %s, %s)", results)	


	def readAll(self):
		self.filesetList = self.db.select("SELECT distinct fileset_id, set_type_code, hash_id FROM bucket_verse_summary", None)
		reader = LPTSExtractReader(self.config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def copyright(self, fileset):
		result = []
		if fileset != None:
			if fileset.Copyrightc() != None:
				result.append("Text: %s" % (fileset.Copyrightc()))
			if fileset.Copyrightp() != None:
				result.append("Audio: %s" % (fileset.Copyrightp()))
			if fileset.Copyright_Video() != None:
				result.append("Video: %s" % (fileset.Copyright_Video()))

		return "\n".join(result)


	def copyrightDate(self, copyright):
		result = None
		if copyright != None:	
			parts = copyright.split(" ")
			for part in parts:
				if len(part) == 4 and part.isdigit():
					result = part
		return result


	def copyrightDescription(self, copyright):
		# use the copyright value 
		return copyright

	def openAccess(self):
		# This defaults to 1, which I think is not correct.
		return 1



config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
filesets = BibleFilesetCopyrightsTable(config, db)
filesets.process()
db.close()




