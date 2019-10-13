# BibleFilesetCopyrightOrganizationsTable.py
#
#
#| hash_id           | char(12)         | NO   | PRI | NULL              |
#| organization_id   | int(10) unsigned | NO   | PRI | NULL              |
#| organization_role | int(11)          | NO   | MUL | NULL              |
#| PRIMARY KEY (hash_id, organization_id)
#| KEY (organization_id)
#| KEY (organization_role)
#| FOREIGN KEY (organization_role) REFERENCES bible_fileset_copyright_roles (id)
#| FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id)
#| FOREIGN KEY (organization_id) REFERENCES organizations (id)
#

import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *


class BibleFilesetCopyrightOrganizationsTable:

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
			(organizationId, organizationRole) = self.organization(filesetId, typeCode)
			results.append((hashId, organizationId, organizationRole))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_fileset_copyright_organizations (hash_id, organization_id, organization_role) VALUES (%s, %s, %s)", results)


	def readAll(self):
		self.filesetList = self.db.select("SELECT distinct fileset_id, set_type_code, hash_id FROM bucket_verse_summary", None)
		self.orgNameMap = self.db.selectMap("SELECT name, organization_id FROM organization_translations", None)
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
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
filesets = BibleFilesetCopyrightOrganizationsTable(config, db)
filesets.process()
db.close()



