# UpdateDBPLPTSTable.py
#
# This program updates DBP tables from the LPTS database source.
# It does INSERT, UPDATE and DELETE of all table rows that need
# to be changed based upon a comparison of DBP to LPTS.
#

import io
import os
import sys
import re
from datetime import datetime
from Config import *
from LPTSExtractReader import *
from SQLUtility import *
from SQLBatchExec import *
from LoadOrganizations import *
#from UpdateDBPBiblesTable import *
from UpdateDBPAccessTable import *

class UpdateDBPLPTSTable:

	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader
		self.updateCounts = {}
		self.hashIdMap = None
		self.sqlLog = []


	def process(self):
		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
		filesetList = self.db.select(sql, ())
		##self.updateAccessGroupFilesets(filesetList)
		access = UpdateDBPAccessTable(self.config, self.db, self.dbOut, self.lptsReader)
		access.process(filesetList)
		self.updateBibleFilesetTags(filesetList)
		self.updateBibleFilesetCopyrights(filesetList)
		self.updateBibleFilesetCopyrightOrganizations()

	##
	## Bible Fileset Tags
	##
	def updateBibleFilesetTags(self, filesetList):
		insertRows = []
		updateRows = []
		deleteRows = []
		adminOnly = 0
		notes = None
		iso = "eng"
		languageId = 6414
		sql = "SELECT hash_id, name, description FROM bible_fileset_tags WHERE language_id = %s"
		tagHashIdMap = {}
		resultSet = self.db.select(sql, (languageId))
		for row in resultSet:
			(hashId, name, description) = row
			tagMap = tagHashIdMap.get(hashId, {})
			tagMap[name] = description
			tagHashIdMap[hashId] = tagMap
		
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			if typeCode != "app":

				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)

				tagMap = tagHashIdMap.get(hashId, {})
				for name in ["bitrate", "sku", "stock_no", "volume"]:
					oldDescription = tagMap.get(name)

					if typeCode == "audio" or name != "bitrate": # do not do bitrate for non-audio

						if lptsRecord != None:
							if name == "bitrate":
								bitrate = filesetId[10:12] if len(filesetId) > 10 else "64"
								description = bitrate + "kbps"
							elif name == "stock_no":
								description = lptsRecord.Reg_StockNumber()
							elif name == "sku":
								description = None # intended to remove all sku
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
							description = description.replace("'", "\\'")
							insertRows.append((description, adminOnly, notes, iso, hashId, name, languageId))

						elif (oldDescription != description):
							description = description.replace("'", "\\'")
							updateRows.append((description, adminOnly, notes, iso, hashId, name, languageId))
							#print("UPDATE: %s %s: OLD %s  NEW: %s" % (filesetId, name, oldDescription, description))

		tableName = "bible_fileset_tags"
		pkeyNames = ("hash_id", "name", "language_id")
		attrNames = ("description", "admin_only", "notes", "iso")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	##
	## Bible Fileset Copyrights
	##
	def updateBibleFilesetCopyrights(self, filesetList):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = ("SELECT hash_id, copyright_date, copyright, copyright_description, open_access"
				" FROM bible_fileset_copyrights")
		copyrightHashIdMap = {}
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			hashId = row[0]
			newRow = row[1:]
			copyrightHashIdMap[hashId] = newRow

		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			if filesetId[8:10] == "SA":
				dbpFilesetId = filesetId[:8] + "DA" + filesetId[10:]
			else:
				dbpFilesetId = filesetId
			#lptsRecords = self.lptsReader.getFilesetRecords(dbpFilesetId)
			row = copyrightHashIdMap.get(hashId, None)

			if typeCode != "app":
				(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, dbpFilesetId)
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
					copyrightMsg = copyrightMsg.replace("'", "\\'")
					insertRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

				elif (row != None and
					(row[0] != copyrightDate or
					row[1] != copyrightMsg or
					row[2] != copyrightMsg or
					row[3] != 1)):
					copyrightMsg = copyrightMsg.replace("'", "\\'")
					updateRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

		tableName = "bible_fileset_copyrights"
		pkeyNames = ("hash_id",)
		attrNames = ("copyright_date", "copyright", "copyright_description", "open_access")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def updateBibleFilesetCopyrightOrganizations(self):
		orgs = LoadOrganizations(self.config, self.db, self.dbOut, self.lptsReader)
		orgs.changeCopyrightOrganizationsPrimaryKey()
		# These methods should be called by Validate
		#unknownLicensors = orgs.validateLicensors()
		#unknownCopyrights = orgs.validateCopyrights()
		sql = "SELECT id, set_type_code, asset_id, hash_id FROM bible_filesets ORDER BY id, set_type_code"
		filesetList = self.db.select(sql, ())
		#print("num filelists", len(filesetList))
		orgs.updateLicensors(filesetList)
		orgs.updateCopyrightHolders(filesetList)


	## This is not currently used.
	def prepareLog(self, tranType, tableName, attrNames, pkeyNames, values):
		if tableName == "bibles":
			self.prepareBiblesLog(tranType, tableName, attrNames, pkeyNames, values)
		else:
			self.prepareFilesetsLog(tranType, tableName, attrNames, pkeyNames, values)


	def prepareBiblesLog(self, tranType, tableName, attrNames, pkeyNames, values):
		#if self.hashIdMap == None:
		#	self.hashIdMap = {}
		#	sql = ("SELECT bf.hash_id, bfc.bible_id, bf.id, bf.set_type_code, bf.set_size_code"
		#		" FROM bible_filesets bf, bible_fileset_connections bfc"
		#		" WHERE bf.hash_id = bfc.hash_id")
		#	resultSet = self.db.select(sql, ())
		#	for (hashId, bibleId, filesetId, setTypeCode, setSizeCode) in resultSet:
		#		self.hashIdMap[hashId] = (bibleId, filesetId, setTypeCode, setSizeCode)
		typeMsg = "%s-%s " % (tableName, tranType)
		numAttr = len(attrNames)
		#hashIdPos = pkeyNames.index("hash_id") + numAttr
		for value in values:
			idMsg = ""
			keyMsg = []
			attrMsg = []
			for index in range(len(value)):
				#if index == hashIdPos:
				#	hashId = value[index]
				#	idMsg = "%s/%s/%s/%s " % self.hashIdMap[hashId]
				#elif index < numAttr:
				if index < numAttr:
					attrMsg.append("%s=%s" % (attrNames[index], str(value[index])))
				else:
					keyMsg.append("%s=%s" % (pkeyNames[index - numAttr], str(value[index])))
			msg = idMsg + typeMsg
			if len(keyMsg) > 0:
				msg += "PKEY: " + ", ".join(keyMsg)
			if len(attrMsg) > 0:
				msg += "  COLS: " + ", ".join(attrMsg)
			self.sqlLog.append(msg)


	def prepareFilesetsLog(self, tranType, tableName, attrNames, pkeyNames, values):
		if self.hashIdMap == None:
			self.hashIdMap = {}
			sql = ("SELECT bf.hash_id, bfc.bible_id, bf.id, bf.set_type_code, bf.set_size_code"
				" FROM bible_filesets bf, bible_fileset_connections bfc"
				" WHERE bf.hash_id = bfc.hash_id")
			resultSet = self.db.select(sql, ())
			for (hashId, bibleId, filesetId, setTypeCode, setSizeCode) in resultSet:
				self.hashIdMap[hashId] = (bibleId, filesetId, setTypeCode, setSizeCode)
		typeMsg = "%s-%s " % (tableName, tranType)
		numAttr = len(attrNames)
		hashIdPos = pkeyNames.index("hash_id") + numAttr
		for value in values:
			idMsg = ""
			keyMsg = []
			attrMsg = []
			for index in range(len(value)):
				if index == hashIdPos:
					hashId = value[index]
					idMsg = "%s/%s/%s/%s " % self.hashIdMap[hashId]
				elif index < numAttr:
					attrMsg.append("%s=%s" % (attrNames[index], str(value[index])))
				else:
					keyMsg.append("%s=%s" % (pkeyNames[index - numAttr], str(value[index])))
			msg = idMsg + typeMsg
			if len(keyMsg) > 0:
				msg += "PKEY: " + ", ".join(keyMsg)
			if len(attrMsg) > 0:
				msg += "  COLS: " + ", ".join(attrMsg)
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
	dbOut = SQLBatchExec(config)
	filesets = UpdateDBPLPTSTable(config, db, dbOut, lptsReader)
	filesets.process()
	db.close()
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute("test-lpts")

"""
truncate table access_group_filesets;
SET FOREIGN_KEY_CHECKS=0;
insert into access_group_filesets 
select * from access_group_filesets_backup;
SET FOREIGN_KEY_CHECKS=1;
select count(*) from access_group_filesets;

truncate table bible_fileset_tags;
SET FOREIGN_KEY_CHECKS=0;
insert into bible_fileset_tags 
select * from bible_fileset_tags_backup;
SET FOREIGN_KEY_CHECKS=1;
select count(*) from bible_fileset_tags;

truncate table bible_fileset_copyrights;
SET FOREIGN_KEY_CHECKS=0;
insert into bible_fileset_copyrights 
select * from bible_fileset_copyrights_backup;
SET FOREIGN_KEY_CHECKS=1;
select count(*) from bible_fileset_copyrights;
"""
