# AccessGroupFilesetsTable.py
#
#
#| access_group_id | int(10) unsigned | NO   | PRI | NULL              |
#| hash_id         | char(12)         | NO   | PRI | NULL              |
#
#
#+----+----------------------+----------+
#| id | name                 | count(*) |
#+----+----------------------+----------+
#|  0 |                      |        0 |
#|  1 | TEST_GROUP           |        0 |
#|  2 | PUBLIC_DOMAIN        |      126 |
#|  3 | FCBH_GENERAL         |     3481 |
#|  4 | FCBH_WEB             |      380 | config.access_video
#|  5 | FCBH_GENERAL_EXCLUDE |        0 |
#|  6 | DBS_GENERAL          |     1633 |
#|  7 | RESTRICTED           |      311 | config.access_restricted
#|  8 | GIDEONS_HIDE         |       19 |
#|  9 | FCBH_HUB             |     5110 | config.access_granted
#| 10 | BIBLEIS_HIDE         |        0 |
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
		self.PERMISSION_PUBLIC_DOMAIN = config.permission_public_domain # 2
		self.PERMISSION_FCBH_GENERAL = config.permission_fcbh_general # 3  Don't know what is for
		self.PERMISSION_FCBH_WEB = config.permission_fcbh_web # 4
		self.PERMISSION_FCBH_GENERAL_EXCLUDE = config.permission_fcbh_general_exclude # 5 NOT USED
		self.PERMISSION_DBS_GENERAL = config.permission_dbs_general # 6
		self.PERMISSION_RESTRICTED = config.permission_restricted # 7
		self.PERMISSION_GIDEONS_HIDE = config.permission_gideons_hide # 8
		self.PERMISSION_FCBH_HUB = config.permission_fcbh_hub # 9
		self.PERMISSION_BIBLEIS_HIDE = config.permission_bibleis_hide # 10


	def readAll(self):
		self.validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		self.filesetList = self.validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		print("num filesets in bible_filesets", len(self.filesetList))
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in LPTS", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		print("num text filesets in LPTS", len(self.textMap.keys()))
		self.videoMap = reader.getVideoMap()
		print("num video filesets in LPTS", len(self.videoMap.keys()))


	def accessGroupIds(self, filesetId, typeCode):
		results = []
		if typeCode == "app":
			record = self.audioMap.get(filesetId)

		elif typeCode == "aud":
			record = self.audioMap.get(filesetId[:10])
			if record != None and record.thisDamIdStatus() == "Live":
				print("inside audio")
				if record.DBPWebHub() == "-1":
					results.append(self.PERMISSION_FCBH_HUB)
				else:
					results.append(self.PERMISSION_RESTRICTED)

		elif typeCode == "tex":
			record = self.textMap.get(filesetId)
			if record != None and record.thisDamIdStatus() == "Live":
				if record.CreativeCommonsText() == "PD":
					results.append(self.PERMISSION_PUBLIC_DOMAIN)
				if record.HubText() == "-1":
					results.append(self.PERMISSION_FCBH_HUB)
				else:
					results.append(self.PERMISSION_RESTRICTED)
			
		elif typeCode == "vid":
			record = self.videoMap.get(filesetId)
			if record != None and record.thisDamIdStatus() == "Live":
				if record.WebHubVideo() == "-1":
					results.append(self.PERMISSION_FCBH_WEB)
				else:
					results.append(self.PERMISSION_RESTRICTED)

		else:
			print("ERROR: Invalid type_code %s" % (typeCode))
			sys.exit()

		if record != None and record.thisDamIdStatus() == "Live":
			version = record.Version()
			if version != None and "Catholic" in version:
				results.append(self.PERMISSION_GIDEONS_HIDE)

		return results


config = Config()
filesets = AccessGroupFilesetsTable(config)
filesets.readAll()
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


"""
		Additional Names in Access, not extracted
						"GBN_Text": "",
						"GBN_Audio": "",
						"FCBHStore": "",
						"MobileVideo": "",
						"GBN_Video": "",
						"DownloadVideo": "",
						"StreamingVideo": "",
						"FCBHStoreVideo": "",
						"ItunesPodcastVideo": "” }

"""



