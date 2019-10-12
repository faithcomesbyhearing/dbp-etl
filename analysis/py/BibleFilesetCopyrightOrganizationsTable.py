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
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

# I think I need to modify this so that it runs using bucket_listing not bible_filesets

class BibleFilesetCopyrightOrganizationsTable:

	def __init__(self, config):
		self.config = config


	def readAll(self):
		self.validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		self.orgNameMap = self.validDB.selectMap("SELECT name, organization_id FROM organization_translations", None)
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def organization(self, filesetId, typeCode):
		result = []
		copyright = None
		# problem: self.orgNameMap can have multiple id's for the same name, but they differ in languageId
		if typeCode == "aud" or typeCode == "app":
			record = self.audioMap.get(filesetId[:10])
			if record != None:
				copyright = record.Copyrightp()
		elif typeCode == "tex":
			record = self.textMap.get(filesetId)
			if record != None:
				copyright = record.Copyrightc()
		elif typeCode == "vid":
			record = self.videoMap.get(filesetId)
			if record != None:
				copyright = record.Copyright_Video()
		else:
			print("ERROR: Invalid type_code %s" % (typeCode))
			sys.exit()
		if copyright != None:
			orgId = self.orgNameMap.get(copyright)
			if orgId != None:
				return (orgId, 1)
			elif copyright.count("Hosanna")>0:
				return (9, 1)
			elif copyright.count("Mitla Studio")>0:
				return (238, 1)
			elif copyright.count("Wycliffe")>0:
				return (30, 1)
			else:
				print("ERROR: filesetId %s has name %s, but there is no id match" % (filesetId, copyright))
		return (1152, 3) # These must really be null
		# There is much more to do for role 2 and role 3


config = Config()
filesets = BibleFilesetCopyrightOrganizationsTable(config)
filesets.readAll()
results = []

for fileset in filesets.filesetList:
	filesetId = fileset[0]
	typeCode = fileset[1][0:3]
	hashId = fileset[2]
	(organizationId, organizationRole) = filesets.organization(filesetId, typeCode)
	results.append((hashId, organizationId, organizationRole))

print("num records to insert %d" % (len(results)))

filesets.validDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_copyright_organizations (hash_id, organization_id, organization_role) VALUES (%s, %s, %s)", results)
outputDB.close()



