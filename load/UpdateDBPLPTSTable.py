# UpdateDBPLPTSTable.py
#
# This program updates DBP tables from the LPTS database source.
# It does INSERT, UPDATE and DELETE of all table rows that need
# to be changed based upon a comparison of DBP to LPTS.
#

import sys
import hashlib
import re
from Config import *
from RunStatus import *
from LanguageReader import *
from SQLUtility import *
from SQLBatchExec import *
from LoadOrganizations import *
from UpdateDBPAccessTable import *
from UpdateDBPBibleTranslations import *
from UpdateDBPLanguageTranslation import *

DefaultLanguageId = 6414
DefaultISO = "eng"

class UpdateDBPLPTSTable:

	@staticmethod
	def getHashId(bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]

	def __init__(self, config, dbOut, languageReader):
		self.config = config
		self.db = SQLUtility(config)
		self.dbOut = dbOut
		# It updates the regex to use the SQL temporary variable as a reference in the creation
		# of the license group and license group filesets
		self.dbOut.UpdateUnquotedRegex(re.compile(r"(.*?)(\'@[^']*?\')(.*$)"))
		self.languageReader = languageReader
		self.updateCounts = {}
		self.hashIdMap = None
		self.sqlLog = []
		self.audioRegex = re.compile("([A-Z0-9]+)-([a-z]+)([0-9]+)")


	def merge_fileset_lists(self, fileset_list_1, fileset_list_2):
		# Dictionary to keep track of items by hash_id to avoid duplicates
		combined_dict = {}

		for item in fileset_list_1:
			(_, _, _, _, _, hash_id) = item
			combined_dict[hash_id] = item

		for item in fileset_list_2:
			(_, _, _, _, _, hash_id) = item
			combined_dict[hash_id] = item

		# Extract the combined items back to a list
		return list(combined_dict.values())

	def process(self):
		RunStatus.printDuration("BEGIN LPTS UPDATE")

		# get the biblefilesets that will be inserted or updated
		filesetList1 = self.upsertBibleFilesetsAndConnections()

		# now, with the full list of bible_filesets, we can continue with normal processing
		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
		filesetList2 = self.db.select(sql, ())

		# we need to use the list of the new bible_filesets because the upsertBibleFilesetsAndConnections have not stored into database the record for now.
		# but we also need to capture the "derived" filesets (eg transcoded opus16) when content is loaded 
		# combine filesetList (from DB) and newFilesetList (which has not been committed yet)
		# this will capture opus16 for new content load as well as new biblefilesets from metadata load
		# Also, some filesets will be in both lists, so remove dups using a set
		# But we need to remove the duplicates by filtering with the hash ID because the fileset could be a little different. For example, the set_size_code could be different, but the hash_id and fileset could be the same value.
		filesetList = self.merge_fileset_lists(filesetList1, filesetList2)


		access = UpdateDBPAccessTable(self.config, self.db, self.dbOut, self.languageReader)
		access.process(filesetList)
		self.updateBibleFilesetTags(filesetList)
		self.updateBibleFilesetCopyrights(filesetList)
		self.updateBibleFilesetCopyrightOrganizations(filesetList)
		bibletranslations = UpdateDBPBibleTranslations(self.config, self.db, self.dbOut, self.languageReader)
		bibletranslations.insertEngVolumeName()
		languageTranslations = UpdateDBPLanguageTranslation(self.config, self.db, self.dbOut, self.languageReader)
		languageTranslations.updateOrInsertlanguageTranslation()
		self.db.close()

	def getBibleFilesetType(self, filesetId):
		if filesetId.endswith('1DA'):
			return 'audio'
		elif filesetId.endswith('2DA'):
			return 'audio_drama'
		elif filesetId.endswith('ET-usx'):
			return 'text_usx'
		elif filesetId.endswith('ET-html'):
			return 'text_html'
		elif filesetId.endswith('ET-json'):
			return 'text_json'
		elif filesetId.endswith('_ET'):
			return 'text_plain'
		elif filesetId.endswith('2DV'):
			return 'video_stream'
		elif filesetId.endswith('1SA'):
			return 'audio_stream'
		elif filesetId.endswith('2SA'):
			return 'audio_drama_stream'

		# Default case if none of the conditions match
		return None

	def getBibleFilesetSizeCode(self, portion):
		if portion == 'Portion(s)':
			return 'NTP'
		elif portion == 'New Testament':
			return 'NT'
		elif portion == 'Old Testament':
			return 'OT'

		# Default case if none of the conditions match
		return None

	##
	## Bible Filesets
	##
	def upsertBibleFilesetsAndConnections(self):
		indexMap = {"audio": [1, 2, 3], "text": [1, 2, 3], "video": [1, 2, 3 ]}

		lptsFilesetList = []
		lptsFilesetProcessed = {}
		for lptsRecord in self.languageReader.resultSet:
			for typeCode in ["audio", "text", "video"]:
				for index in indexMap[typeCode]:
					filesetIds = lptsRecord.DamIds(typeCode, index)
					for filesetId in filesetIds:
						# iterate over each fileset and call upsertBibleFileset
						# need to calculate setSizeCode and SetTypeCode
						bibleId = lptsRecord.DBP_EquivalentByIndex(index)
						setTypeCode = self.getBibleFilesetType(filesetId)
						setSizeCode = self.getBibleFilesetSizeCode(lptsRecord.Portion())
						isArchived = 1 if lptsRecord.IsActive() == False else 0

						if bibleId != None and setTypeCode != None and setSizeCode != None and lptsFilesetProcessed.get(filesetId) == None:
							hashId = self.upsertBibleFileset(dbConn=self.db, setTypeCode=setTypeCode, setSizeCode=setSizeCode, filesetId=filesetId, isArchived=isArchived)
							if hashId != None:
								self.upsertBibleFilesetConnection(self.db, hashId, bibleId)
								bucket = self.config.s3_bucket
								lptsFilesetList.append((bibleId, filesetId, setTypeCode, setSizeCode, bucket, hashId))
								lptsFilesetProcessed[filesetId] = True


		return lptsFilesetList

	def upsertBibleFilesetConnection(self, dbConn, hashId, bibleId):
		insertRows = []

		# We will validate if the bible exist before trying to create the relationship
		bible = dbConn.selectRow("SELECT * FROM bibles WHERE id=%s", (bibleId))
		if bible == None:
			return
		else :
			row = dbConn.selectRow("SELECT * FROM bible_fileset_connections WHERE hash_id=%s AND bible_id=%s", (hashId, bibleId))
			if row == None:
				insertRows.append((hashId, bibleId))
				tableName = "bible_fileset_connections"
				pkeyNames = ("hash_id", "bible_id")
				attrNames = ()
				self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)

	def updateBibleFilesetLicenseGroup(self, inputFileset, hashId):
		row = self.db.selectRow("SELECT lg.id, lg.name FROM license_group lg INNER JOIN bible_filesets bf ON bf.license_group_id = lg.id WHERE bf.id=%s LIMIT 1", (inputFileset.filesetId))

		# Create license group if it does not exist
		if row == None:
			row = self.db.selectRow("SELECT lg.id, lg.name FROM license_group lg INNER JOIN bible_filesets bf ON bf.license_group_id = lg.id WHERE bf.id=%s LIMIT 1", (inputFileset.lptsDamId))

			tableNameFilesets = "bible_filesets"
			pkeyNamesFilesets = ("hash_id",)
			attrNamesToUpdate = ("license_group_id",)

			tableNameGroup = "license_group"
			pkeyNamesGroup = ("name",)
			attrNamesGroup = ("description",)

			updateRowsFilesets = []
			insertRowsGroup = []

			if row != None:
				(licenseId, _) = row
				updateRowsFilesets.append((licenseId, hashId))
			else:
				licenseName = "%s-%s" % (inputFileset.bibleId, inputFileset.typeCode)

				row = self.db.selectRow("SELECT lg.id FROM license_group lg WHERE lg.name=%s LIMIT 1", (licenseName))
				# Check if the license group name exists
				if row == None:
					# Check if the license group name has already been created
					if self.dbOut.hasKeyStatement(licenseName) is False:
						insertRowsGroup.append((licenseName, licenseName))
					licenseNameKey = "@" + SQLBatchExec.sanitize_value(licenseName)
					updateRowsFilesets.append((licenseNameKey, hashId))
				else:
					(licenseId,) = row
					updateRowsFilesets.append((licenseId, hashId))

			self.dbOut.insert(tableNameGroup, pkeyNamesGroup, attrNamesGroup, insertRowsGroup, 0)
			self.dbOut.update(tableNameFilesets, pkeyNamesFilesets, attrNamesToUpdate, updateRowsFilesets)

	def updateBibleProductCode(self, inputFileset, hashId):
		PRODUCT_CODE_AUDIO = "product_code"
		PRODUCT_CODE_VIDEO_TEMPLATE = "product_code:{}"

		existing_tags = self.db.select(
			"SELECT name, description FROM bible_fileset_tags WHERE language_id = %s AND hash_id = %s",
			(DefaultLanguageId, hashId)
		)
		tag_name_map = {name: description for name, description in existing_tags}

		insert_rows, update_rows = [], []
		notes, admin_only, iso = None, 1, DefaultISO

		language_record, _ = self.languageReader.getLanguageRecordLoose(
			inputFileset.typeCode, inputFileset.bibleId, inputFileset.filesetId
		)

		def handle_product_code(tag_name, product_code):
			old_code = tag_name_map.get(tag_name)
			if old_code:
				if old_code != product_code:
					update_rows.append((product_code, admin_only, notes, iso, hashId, tag_name, DefaultLanguageId))
			else:
				insert_rows.append((product_code, admin_only, notes, iso, hashId, tag_name, DefaultLanguageId))

		if inputFileset.typeCode == "audio":
			if inputFileset.audioFileNames():
				product_code = language_record.CalculateProductCode(inputFileset.filesetId, "audio", None)
				handle_product_code(PRODUCT_CODE_AUDIO, product_code)

		elif inputFileset.typeCode == "video":
			books_allowed = self.languageReader.getGospelsAndApostolicHistoryBooks()
			# We need to get the list of books that are allowed for the video fileset (Gospels and Apostolic History)
			if inputFileset.hasGospelAndActFilmsVideo(books_allowed):
				# We need to create the product code for each book
				for book_id in books_allowed:
					if inputFileset.videoFileNamesByBook(book_id):
						tag_name = PRODUCT_CODE_VIDEO_TEMPLATE.format(book_id)
						product_code = language_record.CalculateProductCode(inputFileset.filesetId, "video", book_id)
						handle_product_code(tag_name, product_code)
			else:
				cov_book_id = self.languageReader.getCovenantBookId()
				if inputFileset.videoCovenantFileNames():
					tag_name = PRODUCT_CODE_VIDEO_TEMPLATE.format(cov_book_id)
					product_code = language_record.CalculateProductCode(inputFileset.filesetId, "video", cov_book_id)
					handle_product_code(tag_name, product_code)

		table_name = "bible_fileset_tags"
		pkey_names = ("hash_id", "name", "language_id")
		attr_names = ("description", "admin_only", "notes", "iso")
		self.dbOut.insert(table_name, pkey_names, attr_names, insert_rows)
		self.dbOut.update(table_name, pkey_names, attr_names, update_rows)

	##
	## Bible Fileset Tags
	##
	def updateBibleFilesetTags(self, filesetList):
		hashIds = set()
		# Collect all unique hashIds from the filesetList
		for (_, _, _, _, _, hashId) in filesetList:
			hashIds.add(hashId)

		insertRows = []
		updateRows = []
		deleteRows = []
		adminOnly = 0
		notes = None
		iso = DefaultISO
		languageId = DefaultLanguageId
		tagHashIdMap = {}
		# Get the existing tags for the given hashIds
		resultSet = []
		if len(hashIds) > 0:
			# Fetch existing tags for the specified language and hashIds
			# Using tuple to ensure that hashIds is treated as a single parameter
			# This is necessary to avoid SQL errors when hashIds is empty
			placeholders = ', '.join(['%s'] * len(hashIds))
			query = f"SELECT hash_id, name, description, admin_only FROM bible_fileset_tags WHERE language_id = %s AND hash_id IN ({placeholders})"
			resultSet = self.db.select(
				query,
				(languageId, *hashIds)
			)

		tableName = "bible_fileset_tags"
		pkeyNames = ("hash_id", "name", "language_id")
		attrNames = ("description", "admin_only", "notes", "iso")

		for row in resultSet:
			(hashId, name, description, adminOnly) = row
			tagMap = tagHashIdMap.get(hashId, {})
			tagMap[name] = description
			tagMap["%s_admin_only" % name] = adminOnly
			tagHashIdMap[hashId] = tagMap

		for (bibleId, filesetId, setTypeCode, _, _, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			if typeCode != "app":
				if filesetId[8:10] == "SA":
					dbpFilesetId = filesetId[:8] + "DA" + filesetId[10:]
				else:
					dbpFilesetId = filesetId

				codec = None
				bitrate = None
				if typeCode == "audio":
					if filesetId[8:10] == "SA":
						tagNameList = ["stock_no", "volume"]
						codec = "mp3"
					elif len(filesetId) > 10 and filesetId[10] == "-":
						tagNameList = ["container", "codec", "bitrate", "stock_no", "volume"]
						match = self.audioRegex.match(filesetId)
						if match != None:
							codec = match.group(2)
							bitrate = match.group(3) + "kbps"
					else:
						tagNameList = ["container", "codec", "bitrate", "stock_no", "volume"]
						bitrate = filesetId[10:12] if len(filesetId) > 10 else "64"
						bitrate += "kbps"
						codec = "mp3"
				else:
					tagNameList = ["stock_no", "volume"]

				(languageRecord, _) = self.languageReader.getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId)

				tagMap = tagHashIdMap.get(hashId, {})
				for name in tagNameList:
					adminOnly = 0
					oldDescription = tagMap.get(name)

					if languageRecord != None:
						if name == "container":
							if codec == "aac":
								description = "mp4"
							elif codec == "opus":
								description = "webm"
							elif codec == "mp3":
								description = "mp3"
							else:
								description = None
						elif name == "codec":
							description = codec
						elif name == "bitrate":
							description = bitrate
						elif name == "stock_no":
							stockNumber = languageRecord.StockNumberByFilesetId(dbpFilesetId)
							description = stockNumber if stockNumber != None else languageRecord.Reg_StockNumber()
							adminOnly = 1
							oldAdminOnly = tagMap.get("%s_admin_only" % name)
							# Update the admin_only field if it is different from the old value and exists already a value and it is not None
							if (oldAdminOnly != adminOnly) and (oldAdminOnly != None):
								valuesToUpdate = [("admin_only", adminOnly, oldAdminOnly, hashId, name, languageId)]
								self.dbOut.updateCol("bible_fileset_tags", pkeyNames, valuesToUpdate)
						elif name == "volume":
							description = languageRecord.Volumne_Name()
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

					elif (oldDescription != description) and description != None:
						description = description.replace("'", "\\'")
						updateRows.append((description, adminOnly, notes, iso, hashId, name, languageId))
						# print("UPDATE: filesetId=%s hashId=%s %s: OLD %s  NEW: %s" % (filesetId, hashId, name, oldDescription, description))

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
			#languageRecords = self.languageReader.getFilesetRecords(dbpFilesetId)
			row = copyrightHashIdMap.get(hashId, None)

			if typeCode != "app":
				(languageRecord, lptsIndex) = self.languageReader.getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId)
				if languageRecord != None:
					copyrightText = self.escapeChars(languageRecord.Copyrightc())
					copyrightAudio = self.escapeChars(languageRecord.Copyrightp())
					copyrightVideo = self.escapeChars(languageRecord.Copyright_Video())

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

				if row != None and languageRecord == None:
					deleteRows.append((hashId,))

				elif languageRecord != None and row == None and copyrightMsg != None:
					copyrightMsg = copyrightMsg.replace("'", "\\'")
					insertRows.append((copyrightDate, copyrightMsg, copyrightMsg, 1, hashId))

				elif (row != None and
					copyrightMsg != None and
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


	def updateBibleFilesetCopyrightOrganizations(self, filesetList):
		orgs = LoadOrganizations(self.config, self.db, self.dbOut, self.languageReader)
		# These methods should be called by Validate
		#unknownLicensors = orgs.validateLicensors()
		#unknownCopyrights = orgs.validateCopyrights()
		orgs.update_licensors(filesetList)

	def upsertBibleFileset(self, dbConn, setTypeCode, setSizeCode, filesetId, isContentLoaded=0, isArchived=0):
		# Avoid creating or updating the fileset if both values are true
		if isContentLoaded == True and isArchived == True:
			return None

		tableName = "bible_filesets"
		pkeyNames = ("hash_id",)
		attrNames = ("id", "asset_id", "set_type_code", "set_size_code", "content_loaded", "archived")
		updateRows = []
		bucket = self.config.s3_vid_bucket if setTypeCode == "video_stream" else self.config.s3_bucket	
		hashId = self.getHashId(bucket, filesetId, setTypeCode)
		row = dbConn.selectRow("SELECT id, asset_id, set_type_code, set_size_code, content_loaded, archived FROM bible_filesets WHERE hash_id=%s", (hashId,))
		if row == None:
			updateRows.append((filesetId, bucket, setTypeCode, setSizeCode, isContentLoaded, isArchived, hashId))
			self.dbOut.insert(tableName, pkeyNames, attrNames, updateRows)
			return hashId

		if isContentLoaded == 1:
			attrNamesToUpdate = ("id", "asset_id", "set_type_code", "set_size_code", "content_loaded")
			(_, _, _, _, dbpContentLoaded, _) = row
			if dbpContentLoaded != isContentLoaded:
				updateRows.append((filesetId, bucket, setTypeCode, setSizeCode, isContentLoaded, hashId))
				self.dbOut.update(tableName, pkeyNames, attrNamesToUpdate, updateRows)
				return hashId

		if isArchived == 1:
			(_, _, _, _, _, dbpArchived) = row
			if dbpArchived != isArchived:
				attrNamesToUpdate = ("id", "asset_id", "set_type_code", "set_size_code", "archived")
				updateRows.append((filesetId, bucket, setTypeCode, setSizeCode, isArchived, hashId))
				self.dbOut.update(tableName, pkeyNames, attrNamesToUpdate, updateRows)
				return hashId

		return None


	## This is not currently used.
#	def prepareLog(self, tranType, tableName, attrNames, pkeyNames, values):
#		if tableName == "bibles":
#			self.prepareBiblesLog(tranType, tableName, attrNames, pkeyNames, values)
#		else:
#			self.prepareFilesetsLog(tranType, tableName, attrNames, pkeyNames, values)
#
#
#	def prepareBiblesLog(self, tranType, tableName, attrNames, pkeyNames, values):
#		#if self.hashIdMap == None:
		#	self.hashIdMap = {}
		#	sql = ("SELECT bf.hash_id, bfc.bible_id, bf.id, bf.set_type_code, bf.set_size_code"
		#		" FROM bible_filesets bf, bible_fileset_connections bfc"
		#		" WHERE bf.hash_id = bfc.hash_id")
		#	resultSet = self.db.select(sql, ())
		#	for (hashId, bibleId, filesetId, setTypeCode, setSizeCode) in resultSet:
		#		self.hashIdMap[hashId] = (bibleId, filesetId, setTypeCode, setSizeCode)
#		typeMsg = "%s-%s " % (tableName, tranType)
#		numAttr = len(attrNames)
#		#hashIdPos = pkeyNames.index("hash_id") + numAttr
#		for value in values:
#			idMsg = ""
#			keyMsg = []
#			attrMsg = []
#			for index in range(len(value)):
#				#if index == hashIdPos:
				#	hashId = value[index]
				#	idMsg = "%s/%s/%s/%s " % self.hashIdMap[hashId]
				#elif index < numAttr:
#				if index < numAttr:
#					attrMsg.append("%s=%s" % (attrNames[index], str(value[index])))
#				else:
#					keyMsg.append("%s=%s" % (pkeyNames[index - numAttr], str(value[index])))
#			msg = idMsg + typeMsg
#			if len(keyMsg) > 0:
#				msg += "PKEY: " + ", ".join(keyMsg)
#			if len(attrMsg) > 0:
#				msg += "  COLS: " + ", ".join(attrMsg)
#			self.sqlLog.append(msg)


#	def prepareFilesetsLog(self, tranType, tableName, attrNames, pkeyNames, values):
#		if self.hashIdMap == None:
#			self.hashIdMap = {}
#			sql = ("SELECT bf.hash_id, bfc.bible_id, bf.id, bf.set_type_code, bf.set_size_code"
#				" FROM bible_filesets bf, bible_fileset_connections bfc"
#				" WHERE bf.hash_id = bfc.hash_id")
#			resultSet = self.db.select(sql, ())
#			for (hashId, bibleId, filesetId, setTypeCode, setSizeCode) in resultSet:
#				self.hashIdMap[hashId] = (bibleId, filesetId, setTypeCode, setSizeCode)
#		typeMsg = "%s-%s " % (tableName, tranType)
#		numAttr = len(attrNames)
#		hashIdPos = pkeyNames.index("hash_id") + numAttr
#		for value in values:
#			idMsg = ""
#			keyMsg = []
#			attrMsg = []
#			for index in range(len(value)):
#				if index == hashIdPos:
#					hashId = value[index]
#					idMsg = "%s/%s/%s/%s " % self.hashIdMap[hashId]
#				elif index < numAttr:
#					attrMsg.append("%s=%s" % (attrNames[index], str(value[index])))
#				else:
#					keyMsg.append("%s=%s" % (pkeyNames[index - numAttr], str(value[index])))
#			msg = idMsg + typeMsg
#			if len(keyMsg) > 0:
#				msg += "PKEY: " + ", ".join(keyMsg)
#			if len(attrMsg) > 0:
#				msg += "  COLS: " + ", ".join(attrMsg)
#			self.sqlLog.append(msg)


#	def displayLog(self):
#		errorDir = self.config.directory_errors
#		pattern = self.config.filename_datetime 
#		path = errorDir + "LPTS-Update-" + datetime.today().strftime(pattern) + ".out"
#		logFile = open(path, "w")
#		for message in sorted(self.sqlLog):
#			logFile.write(message + "\n")
#		for countKey in sorted(self.updateCounts.keys()):
#			message = "%s = %d\n" % (countKey, self.updateCounts[countKey])
#			logFile.write(message)
#		logFile.write("%s\n" % path)
#		logFile.close()
#
#		logFile = open(path, "r")
#		for line in logFile:
#			print(line)
#		logFile.close()


	def escapeChars(self, value):
		if value == None:
			return
		return value.replace("'", "''")


if (__name__ == '__main__'):
	from Config import *
	from LanguageReaderCreator import *

	config = Config()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	dbOut = SQLBatchExec(config)
	filesets = UpdateDBPLPTSTable(config, dbOut, languageReader)
	filesets.process()
	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-lpts")


# python3 load/UpdateDBPLPTSTable.py test

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
