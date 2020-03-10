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
from datetime import datetime
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class UpdateDBPLPTSTable:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.updateCounts = {}
		self.statements = []
		self.hashIdMap = None
		self.sqlLog = []


	def process(self):
		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
		try:
			filesetList = self.db.select(sql, ())
			self.updateAccessGroupFilesets(filesetList)
			self.updateBibleFilesetTags(filesetList)
			self.updateBibleFilesetCopyrights(filesetList)
		except Exception as err:
			print("ERROR: %s" % (err))
		self.displayLog()

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
					#self.droppedRecordErrors(typeCode, bibleId, filesetId, setTypeCode, assetId)
			
				for (accessGroupId, lptsName) in accessGroupMap.items():
					accessIdInDBP = accessGroupId in accessSet;
					accessIdInLPTS = lpts.get(lptsName) == "-1"

					if accessIdInLPTS and not accessIdInDBP:
						insertRows.append((hashId, accessGroupId))
					elif accessIdInDBP and not accessIdInLPTS:
						deleteRows.append((hashId, accessGroupId))

		tableName = "access_group_filesets"
		pkeyNames = ("hash_id", "access_group_id")
		self.insert(tableName, (), pkeyNames, insertRows)
		self.delete(tableName, pkeyNames, deleteRows)
		self.execute(tableName, len(insertRows), 0, len(deleteRows))

	## deprecated?
	def droppedRecordErrors(self, typeCode, bibleId, filesetId, setTypeCode, assetId):
		if assetId == "dbs-web":
			return
		recs = self.lptsReader.getFilesetRecords(filesetId)
		if recs != None and len(recs) > 0:
			for (status, rec) in recs:
				if rec.DBP_Equivalent() != bibleId:
					print("ERROR: %s/%s/%s is not in LPTS, but it is in %s in %s." %
						(typeCode, bibleId, filesetId, rec.DBP_Equivalent(), rec.Reg_StockNumber()))
				#elif status not in {"Live", "live"}:
				#	print("WARN: %s/%s/%s is not in LPTS, but status is %s in %s." % 
				#		(typeCode, bibleId, filesetId, status, rec.Reg_StockNumber()))
				elif status in {"Live", "live", None}:
					print("ERROR: %s/%s/%s is not in LPTS, but status is %s in %s." %
						(typeCode, bibleId, filesetId, status, rec.Reg_StockNumber()))	
		else:
			print("ERROR: fileset %s, %s, %s not found in LPTS." % (filesetId, setTypeCode, assetId))


	def deleteOldGroups(self, statements):
		a = 1
		#THIS IS REDUNDANT, normal process with do this?#statements.append(("DELETE FROM access_group_filesets WHERE access_group_id < 100", [()]))
		#statements.append(("DELETE FROM access_group_api_keys WHERE access_group_id < 100", [()]))
		#statements.append(("DELETE FROM access_group_keys WHERE access_group_id < 100", [()]))
		#statements.append(("DELETE FROM access_groups WHERE id < 100", [()]))


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


	## deprecated
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
		#print("%s rows inserted into access_group_api_keys" % (len(apiValues)))
		self.db.executeBatch(apiSql, apiValues)
		#print("%d rows inserted into access_group_keys" % (len(keyValues)))
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
	## Bible Fileset Tags
	##
	def updateBibleFilesetTags(self, filesetList):
		insertRows = []
		updateRows = []
		deleteRows = []
		statements = []
		adminOnly = 0
		notes = None
		iso = "eng"
		languageId = 6414
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			if typeCode != "app":

				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)

				sql = ("SELECT name, description"
					" FROM bible_fileset_tags"
					" WHERE hash_id = %s AND language_id = %s")
				tagMap = self.db.selectMap(sql, (hashId, languageId))
				for name in ["bitrate", "sku", "volume"]:
					oldDescription = tagMap.get(name)

					if typeCode == "audio" or name != "bitrate": # do not do bitrate for non-audio

						if lptsRecord != None:
							if name == "bitrate":
								bitrate = filesetId[10:12] if len(filesetId) > 10 else "64"
								description = bitrate + "kbps"
							elif name == "sku":
								description = lptsRecord.Reg_StockNumber()
								if description != None:
									description = description.replace("/", "")
							elif name == "volume":
								description = lptsRecord.Volumne_Name()
							else:
								print("ERROR: unknown bible_fileset_tags name %s" % (name))
								sys.exit()
						else:
							description = None

						if oldDescription != None and description == None:
							deleteRows.append((hashId, name, languageId))
							#print("DELETE: %s %s %s" % (filesetId, name, oldDescription))

						elif description != None and oldDescription == None:
							insertRows.append((description, adminOnly, notes, iso, hashId, name, languageId))

						elif (oldDescription != description):
							updateRows.append((description, adminOnly, notes, iso, hashId, name, languageId))
							#print("UPDATE: %s %s: OLD %s  NEW: %s" % (filesetId, name, oldDescription, description))

		tableName = "bible_fileset_tags"
		pkeyNames = ("hash_id", "name", "language_id")
		attrNames = ("description", "admin_only", "notes", "iso")
		self.insert(tableName, attrNames, pkeyNames, insertRows)
		self.update(tableName, attrNames, pkeyNames, updateRows)
		self.delete(tableName, pkeyNames, deleteRows)
		self.execute(tableName, len(insertRows), len(updateRows), len(deleteRows))	


	##
	## Bible Fileset Copyrights
	##
	def updateBibleFilesetCopyrights(self, filesetList):
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
				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)
				if lptsRecord != None:
					copyrightText = self.escapeChars(lptsRecord.Copyrightc())
					copyrightAudio = self.escapeChars(lptsRecord.Copyrightp())
					copyrightVideo = self.escapeChars(lptsRecord.Copyright_Video())

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

				if row != None and lptsRecord == None:
					deleteRows.append((hashId,))

				elif lptsRecord != None and row == None:
					insertRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

				elif (row != None and
					(row[0] != copyrightDate or
					row[1] != copyrightMsg or
					row[2] != copyrightMsg or
					row[3] != 1)):
					updateRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

		tableName = "bible_fileset_copyrights"
		pkeyNames = ("hash_id",)
		attrNames = ("copyright_date", "copyright", "copyright_description", "open_access")
		self.insert(tableName, attrNames, pkeyNames, insertRows)
		self.update(tableName, attrNames, pkeyNames, updateRows)
		self.delete(tableName, pkeyNames, deleteRows)
		self.execute(tableName, len(insertRows), len(updateRows), len(deleteRows))


	def insert(self, tableName, attrNames, pkeyNames, values):
		if len(values) > 0:
			names = attrNames + pkeyNames
			valsubs = ['%s'] * len(names)
			sql = "INSERT INTO %s (%s) VALUES (%s)" % (tableName, ", ".join(names), ", ".join(valsubs))
			self.statements.append((sql, values))
			self.prepareLog("INSERT", tableName, attrNames, pkeyNames, values)


	def update(self, tableName, attrNames, pkeyNames, values):
		if len(values) > 0:
			sql = "UPDATE %s SET %s WHERE %s" % (tableName, "=%s, ".join(attrNames) + "=%s", "=%s AND ".join(pkeyNames) + "=%s")
			self.statements.append((sql, values))
			self.prepareLog("UPDATE", tableName, attrNames, pkeyNames, values)


	def delete(self, tableName, pkeyNames, pkeyValues):
		if len(pkeyValues) > 0:
			sql = "DELETE FROM %s WHERE %s" % (tableName, "=%s AND ".join(pkeyNames) + "=%s")
			self.statements.append((sql, pkeyValues))
			self.prepareLog("DELETE", tableName, (), pkeyNames, pkeyValues)


	def execute(self, tableName, numInsert, numUpdate, numDelete):
		self.updateCounts[tableName + "-INSERT:"] = numInsert
		self.updateCounts[tableName + "-UPDATE:"] = numUpdate
		self.updateCounts[tableName + "-DELETE:"] = numDelete
		#self.perRowExecute()
		if len(self.statements) > 0:
			#self.db.displayTransaction(self.statements)
			self.db.executeTransaction(self.statements)
			self.statements = []


	## debug routine
	def perRowExecute(self):
		for (statement, values) in self.statements:
			for value in values:
				print(statement, value)
				self.db.execute(statement, value)


	def prepareLog(self, tranType, tableName, attrNames, pkeyNames, values):
		if self.hashIdMap == None:
			self.hashIdMap = {}
			sql = ("SELECT bf.hash_id, bfc.bible_id, bf.id, bf.set_type_code, bf.set_size_code"
				" FROM bible_filesets bf, bible_fileset_connections bfc"
				" WHERE bf.hash_id = bfc.hash_id")
			resultSet = self.db.select(sql, ())
			for (hashId, bibleId, filesetId, setTypeCode, setSizeCode) in resultSet:
				self.hashIdMap[hashId] = (bibleId, filesetId, setTypeCode, setSizeCode)
		typeMsg = "%s\t%s\t" % (tranType, tableName)
		numAttr = len(attrNames)
		hashIdPos = pkeyNames.index("hash_id") + numAttr
		for value in values:
			idMsg = ""
			keyMsg = ["KEY:"]
			attrMsg = ["NEW:"]
			for index in range(len(value)):
				if index == hashIdPos:
					hashId = value[index]
					idMsg = "%s\t%s\t%s\t%s\t" % self.hashIdMap[hashId]
				elif index < numAttr:
					attrMsg.append(str(value[index]))
				else:
					keyMsg.append(str(value[index]))
			msg = idMsg + typeMsg
			if len(keyMsg) > 1:
				msg += "\t".join(keyMsg)
			if len(attrMsg) > 1:
				msg += "\t".join(attrMsg)
			self.sqlLog.append(msg)


	def displayLog(self):
		errorDir = self.config.directory_errors
		pattern = self.config.filename_datetime 
		path = errorDir + "LPTS-Update-" + datetime.today().strftime(pattern) + ".out"
		logFile = open(path, "w")
		for message in sorted(self.sqlLog):
			logFile.write(message + "\n")
		for countKey in sorted(self.updateCounts.keys()):
			message = "%s = %d\n" % (countKey, self.updateCounts[countKey])
			logFile.write(message)
		logFile.write("%s\n" % path)
		logFile.close()

		logFile = open(path, "r")
		for line in logFile:
			print(line)
		logFile.close()


	def escapeChars(self, value):
		if value == None:
			return
		return value.replace("'", "''")


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	filesets = UpdateDBPLPTSTable(config, db, lptsReader)
	filesets.insertAccessGroups() # temporary
	filesets.process()
	#filesets.accessGroupSymmetricTest()
	db.close()
"""
	db.execute("use dbp_users", ())
	filesets = UpdateDBPLPTSTable(config, db)
	filesets.insertAccessGroupAPIKeys(4, "abcdefghijklmnop", "test", "test", True, True, True, True, True)
"""

