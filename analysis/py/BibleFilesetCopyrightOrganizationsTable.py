# BibleFilesetCopyrightOrganizationsTable.py
#
#
#| hash_id           | char(12)         | NO   | PRI | NULL              |
#| organization_id   | int(10) unsigned | NO   | PRI | NULL              |
#| organization_role | int(11)          | NO   | MUL | NULL              |
#

import io
import os
import sys
import hashlib
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class BibleFilesetCopyrightOrganizationsTable:

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

	def organizationId(self, fileset):
		result = []
		# See main 281-289
		# build map of names from organization_translations, with name as key, and id as value
		# lookup name from Copyrightc (text) Copyrightp (audio), etc
		# These are the copyright holders.  They get a organization_role of 1

		# In order to figure out roles 2 and 3 do a join of this table to oranization_translations
		# in order to get the role associated with the name.

		#if fileset.Copyrightc() != None:
		#	result.append("Text: %s" % (fileset.Copyrightc()))
		#if fileset.Copyrightp() != None:
		#	result.append("Audio: %s" % (fileset.Copyrightp()))
		#if fileset.Copyright_Video() != None:
		#	result.append("Video: %s" % (fileset.Copyright_Video()))

		return "\n".join(result)


	def organizationRole(self, copyright):		
		result = None # 1, 2, 3
		return result


config = Config()
filesets = BibleFilesetCopyrightOrganizationsTable(config) 
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

	oganizationId = filesets.organizationId(lookup)
	organizationRole = filesets.organizationRole(copyright)
	results.append((hashId, organizationId, organizationRole))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_copyright_organizations (hash_id, organization_id, organization_role) VALUES (%s, %s, %s)", results)
outputDB.close()



