# UpdateDBPLPTSTable.py
#
#| access_group_id | int(10) unsigned | NO   | PRI | NULL              |
#| hash_id         | char(12)         | NO   | PRI | NULL              |
#| PRIMARY KEY (access_group_id, hash_id)
#| KEY (hash_id)
#| FOREIGN KEY (access_group_id) REFERENCES access_groups (id)
#| FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id)
#

import io
import os
import sys
import hashlib
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class UpdateDBPLPTSTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		statements = []
		insertRows = []
		deleteRows = []
		## note id > 100 is temporary
		accessGroupMap = self.db.selectMap("SELECT id, description FROM access_groups WHERE id > 100", ())

		lptsReader = LPTSExtractReader(self.config)

		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
		filesetList = self.db.select(sql, ())
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]

			accessSet = self.db.selectSet("SELECT access_group_id FROM access_group_filesets WHERE hash_id = %s", (hashId))

			if typeCode in {"text", "audio", "video"}:
				(lptsRecord, lptsIndex) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				if lptsRecord != None:
					lpts = lptsRecord.record
				else:
					lpts = {}
			
				for (accessGroupId, lptsName) in accessGroupMap.items():
					accessIdInDBP = accessGroupId in accessSet;
					accessIdInLPTS = lpts.get(lptsName) == "-1"

					if accessIdInLPTS and not accessIdInDBP:
						insertRows.append((hashId, accessGroupId))
					if accessIdInDBP and not accessIdInLPTS:
						deleteRows.append((hashId, accessGroupId))

		print("num records to insert %d, num records to delete %d" % (len(insertRows), len(deleteRows)))
		if len(deleteRows) > 0:
			statements.append(("DELETE FROM access_group_filesets WHERE hash_id = %s AND access_group_id = %s", deleteRows))
		if len(insertRows) > 0:
			statements.append(("INSERT INTO access_group_filesets (hash_id, access_group_id) VALUES (%s, %s)", insertRows))
		self.deleteOldGroups(statements)
		self.db.displayTransaction(statements)
		self.db.executeTransaction(statements)


	def deleteOldGroups(self, statements):
		statements.append(("DELETE FROM access_group_filesets WHERE access_group_id < 100", [()]))
		#statements.append(("DELETE FROM access_group_api_keys WHERE access_group_id < 100", [()]))
		#statements.append(("DELETE FROM access_group_keys WHERE access_group_id < 100", [()]))
		statements.append(("DELETE FROM access_groups WHERE id < 100", [()]))

	#
	#def deleteNewGroups(self):
	#	self.db.execute("DELETE FROM access_group_filesets WHERE access_group_id > 100", ())
	#	self.db.execute("DELETE FROM access_groups WHERE id > 100", ())
	#

	def populateAccessGroups(self):
		count = self.db.selectScalar("SELECT count(*) FROM access_groups WHERE id > 100", ())
		if count > 0:
			return
		sql = "INSERT INTO access_groups (id, name, description) VALUES (%s, %s, %s)"
		values = []
		values.append((101, "allow_text_NT_DBP", "DBPText"))
		values.append((102, "allow_text_OT_DBP", "DBPTextOT"))
		values.append((103, "allow_audio_DBP", "DBPAudio"))
		#values.append((105, "allow_video_DBP", NULL))

		values.append((111, "allow_text_WEB", "HubText"))
		values.append((113, "allow_audio_WEB", "DBPWebHub"))
		values.append((115, "allow_video_WEB", "WebHubVideo"))

		values.append((121, "allow_text_API", "APIDevText"))
		values.append((123, "allow_audio_API", "APIDevAudio"))
		values.append((125, "allow_video_API", "APIDevVideo"))

		values.append((131, "allow_text_APP", "MobileText"))
		values.append((133, "allow_audio_APP", "DBPMobile"))
		values.append((135, "allow_video_APP", "MobileVideo"))

		values.append((141, "allow_text_GBA", "GBN_Text"))
		values.append((143, "allow_audio_GBA", "GBN_Audio"))
		values.append((145, "allow_video_GBA", "GBN_Video"))

		values.append((153, "allow_audio_RADIO", "Streaming"))
		values.append((155, "allow_video_RADIO", "StreamingVideo"))

		values.append((163, "allow_audio_ITUNES", "ItunesPodcast"))
		values.append((165, "allow_video_ITUNES", "ItunesPodcastVideo"))

		values.append((173, "allow_audio_SALES", "FCBHStore"))
		values.append((175, "allow_video_SALES", "FCBHStoreVideo"))

		values.append((183, "allow_audio_DOWNLOAD", "Download"))
		self.db.executeBatch(sql, values)


	def populateAccessGroupAPIKeys(self, userId, key, name, description, allowDbp, allowWeb, allowAPI, allowAPP, allowGBA):
		dbpSet = {101, 102, 103}
		webSet = {111, 113, 115}
		apiSet = {121, 123, 125}
		appSet = {131, 133, 135}
		gbaSet = {141, 143, 145}
		
		## Update access_group_api_keys
		sql = ("INSERT INTO user_keys (user_id, `key`, name, description) VALUES (%s, %s, %s, %s)")
		values = (userId, key, name, description)
		keyId = self.db.executeInsert(sql, values)
		#keyId = self.db.lastRow()
		#keyId = self.db.conn.cursor().lastrowid
		print("keyId", keyId)
		apiSql = ("INSERT INTO access_group_api_keys (access_group_id, key_id) VALUES (%s, %s)")
		keySql = ("INSERT INTO access_group_keys (access_group_id, key_id_alt, key_id) VALUES (%s, %s, %s)")

		apiValues = []
		keyValues = []
		if allowDbp:
			for accessId in dbpSet:
				apiValues.append((accessId, keyId))
				keyValues.append((accessId, keyId, key))
		if allowWeb:
			for accessId in webSet:
				apiValues.append((accessId, keyId))
				keyValues.append((accessId, keyId, key))
		if allowAPI:
			for accessId in apiSet:
				apiValues.append((accessId, keyId))
				keyValues.append((accessId, keyId, key))				
		if allowAPP:
			for accessId in apiSet:
				apiValues.append((accessId, keyId))
				keyValues.append((accessId, keyId, key))
		if allowGBA:
			for accessId in gbaSet:
				apiValues.append((accessId, keyId))
				keyValues.append((accessId, keyId, key))
		print("%s rows inserted into access_group_api_keys" % (len(apiValues)))
		self.db.executeBatch(apiSql, apiValues)
		print("%d rows inserted into access_group_keys" % (len(keyValues)))
		self.db.executeBatch(keySql, keyValues)


"""
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	db.execute("use dbp", ())
	filesets = UpdateDBPLPTSTable(config, db)
	#filesets.deleteNewGroups()
	filesets.populateAccessGroups()
	filesets.process()
	db.close()
"""
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	db.execute("use dbp_users", ())
	filesets = UpdateDBPLPTSTable(config, db)
	filesets.populateAccessGroupAPIKeys(4, "abcdefghijklmnop", "test", "test", True, True, True, True, True)


"""
3. Test by writing method that read database, generates LPTS Extract like XML.
4. Also, test the addition, removal and modification of LPTS data
"""


