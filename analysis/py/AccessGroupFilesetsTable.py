# AccessGroupFilesetsTable.py
#
#
#| access_group_id | int(10) unsigned | NO   | PRI | NULL              |
#| hash_id         | char(12)         | NO   | PRI | NULL              |
#| PRIMARY KEY (access_group_id, hash_id)
#| KEY (hash_id)
#| FOREIGN KEY (access_group_id) REFERENCES access_groups (id)
#| FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id)
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

	def __init__(self, config, db):
		self.config = config
		self.db = db
		#self.PERMISSION_PUBLIC_DOMAIN = config.permission_public_domain # 2
		#self.PERMISSION_FCBH_GENERAL = config.permission_fcbh_general # 3  Don't know what is for
		#self.PERMISSION_FCBH_WEB = config.permission_fcbh_web # 4
		#self.PERMISSION_FCBH_GENERAL_EXCLUDE = config.permission_fcbh_general_exclude # 5 NOT USED
		#self.PERMISSION_DBS_GENERAL = config.permission_dbs_general # 6
		#self.PERMISSION_RESTRICTED = config.permission_restricted # 7
		#self.PERMISSION_GIDEONS_HIDE = config.permission_gideons_hide # 8
		#self.PERMISSION_FCBH_HUB = config.permission_fcbh_hub # 9
		#self.PERMISSION_BIBLEIS_HIDE = config.permission_bibleis_hide # 10


	def process(self):
		self.readAll()
		results = []

		for fileset in self.filesetList:
			filesetId = fileset[0]
			typeCode = fileset[1][0:3]
			hashId = fileset[2]

			accessGroupIds = self.accessGroupIds(filesetId, typeCode)
			for accessGroupId in accessGroupIds:
				results.append((accessGroupId, hashId))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO access_group_filesets (access_group_id, hash_id) VALUES (%s, %s)", results)


	def readAll(self):
		self.filesetList = self.db.select("SELECT distinct fileset_id, set_type_code, hash_id FROM bucket_verse_summary", None)
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


	def populateAccessGroups(self):
		sql = "INSERT INTO access_groups (id, name, description) VALUES (%s, %s, %s)"
		values = []
		values.append((101, "allow_text_NT_DBP", "Allow NT Text on DBP (lpts: DBPText)"))
		values.append((102, "allow_text_OT_DBP", "Allow OT Text on DBP (lpts: DBPTextOT)"))
		values.append((103, "allow_audio_DBP", "Allow Audio on DBP (lpts: DBPAudio)"))
		values.append((105, "allow_video_DBP", "Allow Video on DBP (lpts: - none -)"))

		values.append((111, "allow_text_WEB", "Allow Text on Bible.is Website (lpts: HubText)"))
		values.append((113, "allow_audio_WEB", "Allow Audio on Bible.is Website (lpts: DBPWebHub)"))
		values.append((115, "allow_video_WEB", "Allow Video on Bible.is Website (lpts: WebHubVideo)"))

		values.append((121, "allow_text_API", "Allow Text use through API (lpts: APIDevText)"))
		values.append((123, "allow_audio_API", "Allow Audio use through API (lpts: APIDevAudio)"))
		values.append((125, "allow_video_API", "Allow for Video use through API (lpts: APIDevVideo)"))

		values.append((131, "allow_text_APP", "Allow Text on Bible.is App (lpts: MobileText)"))
		values.append((133, "allow_audio_APP", "Allow Audio on Bible.is App (lpts: DBPMobile)"))
		values.append((135, "allow_video_APP", "Allow Video on Bible.is App (lpts: MobileVideo)"))

		values.append((141, "allow_text_GBA", "Allow Text in a Global Bible App (lpts: GBN_Text)"))
		values.append((143, "allow_audio_GBA", "Allow Audio in a Global Bible App (lpts: GBN_Audio)"))
		values.append((145, "allow_video_GBA", "Allow for Video in a Global Bible App (lpts: GBN_Video)"))

		values.append((153, "allow_audio_RADIO", "Allow Audio Radio Streaming (lpts: Streaming)"))
		values.append((155, "allow_video_RADIO", "Allow Video Radio Streaming (lpts: StreamingVideo)"))

		values.append((163, "allow_audio_ITUNES", "Allow Audio on iTunes Podcast (lpts: ItunesPodcast)"))
		values.append((165, "allow_video_ITUNES", "Allow Video on iTunes Podcast (lpts: ItunesPodcastVideo)"))

		values.append((173, "allow_audio_SALES", "Allow Audio Sale (lpts: FCBHStore)"))
		values.append((175, "allow_video_SALES", "Allow Video Sales (lpts: FCBHStoreVideo)"))

		values.append((183, "allow_audio_DOWNLOAD", "Allow Audio Download (lpts: Download)"))
		values.append((185, "allow_video_DOWNLOAD", "Allow Video Download (lpts: DownloadVideo)"))
		self.db.executeBatch(sql, values)


config = Config()
db = SQLUtility(config)
filesets = AccessGroupFilesetsTable(config, db)
filesets.populateAccessGroups()
#filesets.process()
db.close()


