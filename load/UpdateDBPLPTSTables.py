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
import re
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class UpdateDBPLPTSTable:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader


	def process(self):
		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
		filesetList = self.db.select(sql, ())
		self.updateAccessGroupFilesets(filesetList)
		self.updateBibleFilesetCopyrights(filesetList)

	##
	## Access Group Filesets
	##
	def updateAccessGroupFilesets(self, filesetList):
		statements = []
		insertRows = []
		deleteRows = []
		## note id > 100 is temporary
		accessGroupMap = self.db.selectMap("SELECT id, description FROM access_groups WHERE id > 100", ())

		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]

			accessSet = self.db.selectSet("SELECT access_group_id FROM access_group_filesets WHERE hash_id = %s", (hashId))

			if typeCode != "app":
				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				if lptsRecord != None:
					lpts = lptsRecord.record
				else:
					lpts = {}
			
				for (accessGroupId, lptsName) in accessGroupMap.items():
					accessIdInDBP = accessGroupId in accessSet;
					accessIdInLPTS = lpts.get(lptsName) == "-1"

					if accessIdInLPTS and not accessIdInDBP:
						insertRows.append((hashId, accessGroupId))
					elif accessIdInDBP and not accessIdInLPTS:
						deleteRows.append((hashId, accessGroupId))

		print("num access_group_filesets to insert %d, num to delete %d" % (len(insertRows), len(deleteRows)))
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

	def insertAccessGroups(self):
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


	def insertAccessGroupAPIKeys(self, userId, key, name, description, allowDbp, allowWeb, allowAPI, allowAPP, allowGBA):
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


	def accessGroupSymmetricTest(self):
		sql = ("SELECT distinct bfc.bible_id, agf.access_group_id, ag.description"
			" FROM bible_fileset_connections bfc, bible_filesets bf, access_group_filesets agf, access_groups ag"
			" WHERE bfc.hash_id = bf.hash_id"
			" AND bf.hash_id = agf.hash_id"
			" AND agf.access_group_id = ag.id"
			" ORDER BY bfc.bible_id, ag.description")
		resultSet = self.db.select(sql, ())
		with open("test_DBP.txt", "w") as outFile:
			priorKey = None
			for (bibleId, accessGroupId, lptsName) in resultSet:
				key = bibleId
				#key = "%s/%s" % (bibleId, filesetId)
				if key != priorKey:
					outFile.write("<DBP_Equivalent>%s</DBP_Equivalent>\n" % (bibleId))
					priorKey = key
				outFile.write("\t<%s>-1</%s>\n" % (lptsName, lptsName))

		with open(self.config.filename_lpts_xml, "r") as lpts:
			results = {}
			permissions = set()
			hasLive = False
			for line in lpts:
				if "<DBP_Eq" in line:
					if len(permissions) > 0 and hasLive:
						results[bibleId] = permissions
					permissions = set()
					hasLive = False
					pattern = re.compile("<DBP_Equivalent[123]?>([A-Z0-9]+)</DBP_Equivalent[123]?>")
					found = pattern.search(line)
					if found != None:
						bibleId = found.group(1)
				if ">-1<" in line:
					permissions.add(line.strip())
				if ">Live<" in line or ">live<" in line:
					hasLive = True
		with open("test_LPTS.txt", "w") as outFile:
			for bibleId in sorted(results.keys()):
				outFile.write("<DBP_Equivalent>%s</DBP_Equivalent>\n" % (bibleId))
				for permission in sorted(results[bibleId]):
					outFile.write("\t%s\n" % (permission))

	##
	## Bible Fileset Copyrights
	##
	def updateBibleFilesetCopyrights(self, filesetList):
		## primary key is hash_id
		insertRows = []
		updateRows = []
		deleteRows = []
		statements = []
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]

			sql = ("SELECT copyright_date, copyright, copyright_description, open_access"
				" FROM bible_fileset_copyrights WHERE hash_id = %s")
			row = self.db.selectRow(sql, (hashId))

			if typeCode != "app":
				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				if lptsRecord != None:
					copyrightText = lptsRecord.Copyrightc()
					copyrightAudio = lptsRecord.Copyrightp()
					copyrightVideo = lptsRecord.Copyright_Video()

					if typeCode == "text":
						copyright = copyrightText
						copyrightMsg = copyrightText
					elif typeCode == "audio":
						copyright = copyrightAudio
						copyrightMsg = "Text: %s\nAudio: %s" % (copyrightText, copyrightAudio)
					elif typeCode == "video":
						copyright = copyrightVideo
						copyrightMsg = "Text: %s\nAudio: %s\nVideo: %s" % (copyrightText, copyrightAudio, copyrightVideo)

					copyrightDate = None
					if copyright != None:
						datePattern = re.compile("([0-9]+)")
						year = datePattern.search(copyright)
						if year != None:
							copyrightDate = year.group(1)
							## Should I work on finding multiple dates?
					#values = (hashId, copyrightDate, copyrightMsg, "")
					#self.statements.append((sql, [values]))

				if row != None and lptsRecord == None:
					deleteRows.append((hashId,))

				elif lptsRecord != None and row == None:
					insertRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

				## This needs to come back after testing
				#elif (row != None and
				#	(row[0] != copyrightDate
				#	or row[1] != copyrightMsg
				#	or row[2] != copyrightMsg
				#	or row[3] != 1)):
				elif (row != None and
					(row[1] != copyrightMsg
					or row[2] != copyrightMsg
					or row[3] != 1)):
					updateRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

		print("num bible_fileset_copyright to insert %d, num to update %s, num to delete %d" % (len(insertRows), len(updateRows), len(deleteRows)))
		if len(insertRows) > 0:
			sql = ("INSERT INTO bible_fileset_copyrights(copyright_date, copyright,"
				" copyright_description, open_access, hash_id) VALUES (%s, %s, %s, %s, %s)")
			statements.append((sql, insertRows))
		if len(updateRows) > 0:
			sql = ("UPDATE bible_fileset_copyrights SET copyright_date =%s, copyright = %s,"
				" copyright_description = %s, open_access = %s WHERE hash_id = %s")
			statements.append((sql, updateRows))
		if len(deleteRows) > 0:
			sql = ("DELETE FROM bible_fileset_copyrights WHERE hash_id = %s")
			statements.append((sql, deleteRows))
		self.db.displayTransaction(statements)
		#self.db.executeTransaction(statements)


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	db.execute("use dbp", ())
	filesets = UpdateDBPLPTSTable(config, db, lptsReader)
	filesets.insertAccessGroups() # temporary
	#filesets.deleteNewGroups()
	filesets.process()
	filesets.accessGroupSymmetricTest()
	db.close()
"""
	db.execute("use dbp_users", ())
	filesets = UpdateDBPLPTSTable(config, db)
	filesets.insertAccessGroupAPIKeys(4, "abcdefghijklmnop", "test", "test", True, True, True, True, True)
"""


"""
4. Also, test the addition, removal and modification of LPTS data
"""


