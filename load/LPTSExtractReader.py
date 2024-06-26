# LPTSExtractReader
#
# This program reads the LPTS Extract and produces hash tables.
#

import sys
import os
import re
from xml.dom import minidom
from LanguageReader import LanguageReaderInterface, LanguageRecordInterface

class LPTSExtractReader (LanguageReaderInterface):

	def __init__(self, lptsExtractPath):
		self.lptsExtractPath = lptsExtractPath
		self.resultSet = []
		dom = None
		try:
			doc = minidom.parse(lptsExtractPath)
		except Exception as err:
			print("FATAL ERROR parsing LPTS extract", err)
			sys.exit(1)
		root = doc.childNodes
		if len(root) != 1 or root[0].nodeName != "dataroot":
			print ("ERROR: First node of LPTS Export must be <docroot>")
			sys.exit()
		else:
			for recNode in root[0].childNodes:
				if recNode.nodeType == 1:
					if recNode.nodeName != "qry_dbp4_Regular_and_NonDrama":
						print("ERROR: Child nodes in LPTS Export must be 'qry_dbp4_Regular_and_NonDrama'")
						sys.exit()
					else:
						resultRow = {}

						for fldNode in recNode.childNodes:
							if fldNode.nodeType == 1:
								# print(fldNode.nodeName + " = " + fldNode.firstChild.nodeValue)
								resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue
						#print("\n\n\n *** creating an languageRecord object ***\n\n\n")
						self.resultSet.append(LanguageRecord(resultRow))
		self.checkRecordCount()
		self.bibleIdMap = self.getBibleIdMap()
		self.filesetIdMap = None
		self.filesetIdMap10 = None
		self.stockNumMap = None
		print("LPTS Extract with %d records and %d BibleIds is loaded." % (len(self.resultSet), len(self.bibleIdMap.keys())))


	# Check the record count against prior run to make certain we have the entire file. And save new record count.
	# Check the record count against prior run to make certain we have the entire file. And save new record count.
	def checkRecordCount(self):
		try:
			filename = "/tmp/dbp-etl-LPTS_count.txt"
			nowCount = len(self.resultSet)
			if os.path.isfile(filename):
				with open(filename, "r") as cntIn:
					priorCount = cntIn.read().strip()
					if priorCount.isdigit():
						if (int(priorCount) - nowCount) > 50:
							print("FATAL: %s is too small. Prior run was %s records. Now it has %d records. This count is stored at %s" 
								% (self.lptsExtractPath, priorCount, nowCount, filename))
							sys.exit()
						else:
							print("LPTS Size. Prior: %s, Current: %s" % (priorCount, nowCount))
			else:
				print("first run of LPTS Reader -- did not find %s" % (filename))
				
			with open(filename, "w") as cntOut:
				cntOut.write(str(nowCount))
				#print("wrote count %s to file %s" % (nowCount, filename))
		except FileNotFoundError:
			print("Exception: first run of LPTS Reader -- did not find %s" % (filename))

    ## Generates Map bibleId: [(index, languageRecord)], called by class init
	def getBibleIdMap(self):
		bibleIdMap = {}
		for rec in self.resultSet:
			bibleId = rec.DBP_Equivalent()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((1, rec))
				bibleIdMap[bibleId] = records
			bibleId = rec.DBP_Equivalent2()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((2, rec))
				bibleIdMap[bibleId] = records
			bibleId = rec.DBP_Equivalent3()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((3, rec))
				bibleIdMap[bibleId] = records
		return bibleIdMap


	## Returns the LPTS Record for a stockNumber
	def getByStockNumber(self, stockNumber):
		if self.stockNumMap == None:
			self.stockNumMap = {}
			for rec in self.resultSet:
				stockNum = rec.Reg_StockNumber()
				if stockNum != None:
					if self.stockNumMap.get(stockNum) == None:
						self.stockNumMap[stockNum] = rec
					else:
						print("ERROR: Duplicate Stock Num", stockNum);
				else:
					print("INFO: the following record has no stock number: EthName: %s and AltName: %s" % (rec.EthName(), rec.AltName()))
		return self.stockNumMap.get(stockNumber)


	## Returns one (record, index) for typeCode, bibleId, filesetId
	## This is a strict method that only returns when BibleId matches and status is Live
	def getLanguageRecord(self, typeCode, bibleId, filesetId):
		normFilesetId = filesetId[:10]
		languageRecords = self.bibleIdMap.get(bibleId)
		if languageRecords != None:
			for (index, record) in languageRecords:
				damIdSet = record.DamIds(typeCode, index)
				if normFilesetId in damIdSet:
					return (record, index)
		return (None, None)


	## This method is a hack, because in LPTS a text damId is 10 char, 
	## but in DBP it is 6 char, when searching LPTS the same text damid
	## can be found in multiple record, but there is no way to know
	## which is correct.
	def getLanguageRecordLoose(self, typeCode, bibleId, filesetId):
		result = self.getLanguageRecord(typeCode, bibleId, filesetId)
		if result[0] != None:
			return result
		result = self.getFilesetRecords(filesetId)
		if result == None or len(result) == 0:
			return (None, None)
		for (status, record) in result:
			if status == "Live":
				return (record, None)
		first = result[0]
		record = first[1]
		return (record, None)


	## This method returns all LPTS records regardless of the status
	## Otherwise it is exactly like getLanguageRecord
	## Return [(record, index)]
	def getLanguageRecordsAll(self, typeCode, bibleId, filesetId):
		results = []
		normFilesetId = filesetId[:10]
		languageRecords = self.bibleIdMap.get(bibleId)
		if languageRecords != None:
			for (index, record) in languageRecords:
				damIdList = record.DamIdMap(typeCode, index)
				if normFilesetId in damIdList:
					#return(record, index)
					results.append((record, index))
		#return (None, None)
		return results		


	## This is a more permissive way to get LPTS Records, it does not require
	## a type or bibleId.  So, it can only be used for non-index fields
	## It returns an array of statuses and records, i.e. [(status, languageRecord)]
	def getFilesetRecords(self, filesetId):
		if self.filesetIdMap == None:
			self.filesetIdMap = {}


			for languageRecord in self.resultSet:
				record = languageRecord.record
				damIdDict = dict(
					list(LanguageRecord.audio1DamIdDict.items()) + 
					list(LanguageRecord.audio2DamIdDict.items()) + 
					list(LanguageRecord.audio3DamIdDict.items()) +
					list(LanguageRecord.text1DamIdDict.items()) + 
					list(LanguageRecord.text2DamIdDict.items()) + 
					list(LanguageRecord.text3DamIdDict.items()) +
					list(LanguageRecord.videoDamIdDict.items()) 
					)
				hasKeys = set(damIdDict.keys()).intersection(set(record.keys()))
				for key in hasKeys:
					statusKey = damIdDict[key]
					damId = record[key]
					if "Text" in key:
						damId = damId[:6]
					status = record.get(statusKey)
					statuses = self.filesetIdMap.get(damId, [])
					statuses.append((status, languageRecord))
					self.filesetIdMap[damId] = statuses
					if "Text" in key: # Put in second key for Text filesets with underscore
						damId = record[key]
						damId = LanguageRecordInterface.transformToTextId(damId)
						statuses = self.filesetIdMap.get(damId, [])
						statuses.append((status, languageRecord))
						self.filesetIdMap[damId] = statuses
		return self.filesetIdMap.get(filesetId[:10], None)


	## This method is different than getFilesetRecords in that it expects an entire 10 digit text filesetId
	## It also removes excess characters for filesets and corrects for SA types.
	## This method does not return the 1, 2, 3 index
	## It returns an array of statuses and records, i.e. Set((languageRecord, status, fieldName))
	def getFilesetRecords10(self, filesetId):	
		if self.filesetIdMap10 == None:
			self.filesetIdMap10 = {}
			damIdDict = dict(
					list(LanguageRecord.audio1DamIdDict.items()) + 
					list(LanguageRecord.audio2DamIdDict.items()) + 
					list(LanguageRecord.audio3DamIdDict.items()) + 
					list(LanguageRecord.text1DamIdDict.items()) + 
					list(LanguageRecord.text2DamIdDict.items()) +
					list(LanguageRecord.text3DamIdDict.items()) +
					list(LanguageRecord.videoDamIdDict.items())
					)
			for languageRecord in self.resultSet:
				record = languageRecord.record
				hasKeys = set(damIdDict.keys()).intersection(set(record.keys()))
				for key in hasKeys:
					statusKey = damIdDict[key]
					damId = record[key]
					status = record.get(statusKey)
					statuses = self.filesetIdMap10.get(damId, set())
					statuses.add((languageRecord, status, key))
					self.filesetIdMap10[damId] = statuses
		damId = filesetId[:10]
		if len(damId) == 10 and damId[-2:] == "SA":
			damId = damId[:8] + "DA";	
		return self.filesetIdMap10.get(damId, None)


	## This method returns a list of field names for development the purposes.
	def getFieldNames(self):
		nameCount = {}
		for rec in self.resultSet:
			for field in rec.record.keys():
				count = nameCount.get(field, 0)
				count += 1
				nameCount[field] = count
		for field in sorted(nameCount.keys()):
			count = nameCount[field]
			print(field, count)


	## This method is used to reduce copyright messages to a shorter string that
	## will be matched with copyrights in the table lpts_copyright_organizations
	## in order to determine the organization_id for the copyright holder.
	def reduceCopyrightToName(self, name):
		pos = 0
		if "©" in name:
			pos = name.index("©")
		elif "℗" in name:
			pos = name.index("℗")
		elif "®" in name:
			pos = name.index("®")
		if pos > 0:
			name = name[pos:]
		for char in ["©", "℗", "®","0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ",", "/", "-", ";", "_", ".", "\t", "\n"]:
			name = name.replace(char, "")
		name = name.replace("     ", " ")
		name = name.replace("    ", " ")
		name = name.replace("   ", " ")
		name = name.replace("  ", " ")
		name = name[:64]
		return name.strip()

class LanguageRecord (LanguageRecordInterface):

	Reg_StockNumberDamIdIndex = {
		"Reg_CAudioDAMID1": True,
		"Reg_NTAudioDamID1": True,
		"Reg_OTAudioDamID1": True,
		"Reg_CAudioDamID2": True,
		"Reg_NTAudioDamID2": True,
		"Reg_OTAudioDamID2": True,
		"Reg_CAudioDamID3": True,
		"Reg_NTAudioDamID3": True,
		"Reg_OTAudioDamID3": True,
		"Reg_NTTextDamID1": True,
		"Reg_OTTextDamID1": True,
		"Reg_NTTextDamID2": True,
		"Reg_OTTextDamID2": True,
		"Reg_NTTextDamID3": True,
		"Reg_OTTextDamID3": True
	}

	ND_StockNumberDamIdIndex = {
		"ND_CAudioDAMID1":  True,
		"ND_NTAudioDamID1": True,
		"ND_OTAudioDamID1": True,
		"ND_CAudioDamID2":  True,
		"ND_NTAudioDamID2": True,
		"ND_OTAudioDamID2": True,
		"ND_CAudioDamID3":  True,
		"ND_NTAudioDamID3": True,
		"ND_OTAudioDamID3": True,
		"ND_NTTextDamID1":  True,
		"ND_OTTextDamID1":  True,
		"ND_NTTextDamID2":  True,
		"ND_OTTextDamID2":  True,
		"ND_NTTextDamID3":  True,
		"ND_OTTextDamID3":  True
	}

	audio1DamIdDict = {
		"ND_CAudioDAMID1": 		"ND_CAudioDAMStatus",
		"ND_NTAudioDamID1": 	"ND_NTAudioDamIDStatus1",
		"ND_OTAudioDamID1": 	"ND_OTAudioDamIDStatus1",
		"Reg_CAudioDAMID1":		"Reg_CAudioDAMIDStatus1",
		"Reg_NTAudioDamID1": 	"Reg_NTAudioDamIDStatus1",
		"Reg_OTAudioDamID1": 	"Reg_OTAudioDamIDStatus1",
		"CAudioDamStockNo":		"CAudioDamStatus", # added 4/17/2020
		"CAudioDAMID1":			"CAudioStatus1" # added 4/17/2020
	}
	audio2DamIdDict = {
		"ND_CAudioDamID2": 		"ND_CAudioDamIDStatus2", # No occurrances 2/20/2020
		"ND_NTAudioDamID2": 	"ND_NTAudioDamIDStatus2",
		"ND_OTAudioDamID2": 	"ND_OTAudioDamIDStatus2",
		"Reg_CAudioDamID2": 	"Reg_CAudioDamIDStatus2", # No occurrances 2/20/2020
		"Reg_NTAudioDamID2": 	"Reg_NTAudioDamIDStatus2",
		"Reg_OTAudioDamID2": 	"Reg_OTAudioDamIDStatus2"
	}
	audio3DamIdDict = { # No occurrances 2/20/2020
		"ND_CAudioDamID3": 		"ND_CAudioDamIDStatus3",
		"ND_NTAudioDamID3": 	"ND_NTAudioDamIDStatus3",
		"ND_OTAudioDamID3": 	"ND_OTAudioDamIDStatus3",
		"Reg_CAudioDamID3": 	"Reg_CAudioDamIDStatus3",
		"Reg_NTAudioDamID3":	"Reg_NTAudioDamIDStatus3",
		"Reg_OTAudioDamID3":	"Reg_OTAudioDamIDStatus3"
	}
	text1DamIdDict = {
		"ND_NTTextDamID1": 		"ND_NTTextDamIDStatus1",
		"ND_OTTextDamID1": 		"ND_OTTextDamIDStatus1",
		"Reg_NTTextDamID1": 	"Reg_NTTextDamIDStatus1",
		"Reg_OTTextDamID1": 	"Reg_OTTextDamIDStatus1"
	}
	text2DamIdDict = {
		"ND_NTTextDamID2": 		"ND_NTTextDamIDStatus2",
		"ND_OTTextDamID2": 		"ND_OTTextDamIDStatus2",
		"Reg_NTTextDamID2": 	"Reg_NTTextDamIDStatus2",
		"Reg_OTTextDamID2": 	"Reg_OTTextDamIDStatus2"
	}
	text3DamIdDict = {
		"ND_NTTextDamID3": 		"ND_NTTextDamIDStatus3",
		"ND_OTTextDamID3": 		"ND_OTTextDamIDStatus3",
		"Reg_NTTextDamID3": 	"Reg_NTTextDamIDStatus3",
		"Reg_OTTextDamID3": 	"Reg_OTTextDamIDStatus3"
	}
	videoDamIdDict = {
		"Video_John_DamStockNo": "Video_John_DamStatus",
		"Video_Luke_DamStockNo": "Video_Luke_DamStatus",
		"Video_Mark_DamStockNo": "Video_Mark_DamStatus",
		"Video_Matt_DamStockNo": "Video_Matt_DamStatus",
		"Video_DamStockNo2": "Video_DamStatus2"
	}

	def __init__(self, record):
		self.record = record

	def recordLen(self):
		return len(self.record.keys())

	## Return the damId's of a record in a set of damIds, which are Live
	def DamIds(self, typeCode, index):
		damIdMap = self.DamIdMap(typeCode, index)
		results = set()
		for (damId, status) in damIdMap.items():
			if status in {"Live", "live", None}:
				results.add(damId)
		return results

	## Return a map of {damId: status} of the damIds in a record
	def DamIdMap(self, typeCode, index):
		if not typeCode in {"audio", "text", "video"}:
			print("ERROR: Unknown typeCode '%s', audio, text, or video is expected." % (typeCode))
		if not index in {1, 2, 3}:
			print("ERROR: Unknown DamId index '%s', 1,2, or 3 expected." % (index))
			sys.exit()
		if typeCode == "audio":
			if index == 1:
				damIdDict = LanguageRecord.audio1DamIdDict
			elif index == 2:
				damIdDict = LanguageRecord.audio2DamIdDict
			elif index == 3:
				damIdDict = LanguageRecord.audio3DamIdDict
		elif typeCode == "text":
			if index == 1:
				damIdDict = LanguageRecord.text1DamIdDict
			elif index == 2:
				damIdDict = LanguageRecord.text2DamIdDict
			elif index == 3:
				damIdDict = LanguageRecord.text3DamIdDict
		elif typeCode == "video":
			damIdDict = LanguageRecord.videoDamIdDict
		else:
			damIdDict = {}
		hasKeys = set(damIdDict.keys()).intersection(set(self.record.keys()))
		results = {}
		for key in hasKeys:
			statusKey = damIdDict[key]
			damId = self.record[key]
			if typeCode == "text":
				damId = damId[:6]
			status = self.record.get(statusKey)
			if results.get(damId) == None or status in {"Live", "live"}:
				results[damId] = status
				if typeCode == "text":
					damId = self.record[key]
					damId = damId[:7] + "_" + damId[8:10]
					results[damId] = status
		return results


	## This method is used to discover DamIds in a record when it is the stockNo that is known
	## It returns a list of tuples (DamId, index, status, fieldName)
	# BWF -part of LanguageRecord interface
	def DamIdList(self, typeCode):
		if not typeCode in {"audio", "text", "video"}:
			print("ERROR: Unknown typeCode '%s', audio, text, or video is expected." % (typeCode))
		if typeCode == "audio":
			damIdDict = dict(
					list(LanguageRecord.audio1DamIdDict.items()) +
					list(LanguageRecord.audio2DamIdDict.items()) +
					list(LanguageRecord.audio3DamIdDict.items())
					)
		elif typeCode == "text":
			damIdDict = dict(
					list(LanguageRecord.text1DamIdDict.items()) +
					list(LanguageRecord.text2DamIdDict.items()) +
					list(LanguageRecord.text3DamIdDict.items())
					)
		elif typeCode == "video":
			damIdDict = LanguageRecord.videoDamIdDict
		else:
			damIdDict = {}
		hasKeys = set(damIdDict.keys()).intersection(set(self.record.keys()))
		results = []
		for key in hasKeys:
			statusKey = damIdDict[key]
			damId = self.record[key]
			status = self.record.get(statusKey)
			if "3" in key:
				index = 3
			elif "2" in key:
				index = 2
			else:
				index = 1
			results.append((damId, index, status, key))
		return results


	## This method reduces text fileset tuples that are produced by DamIdList by removing the 8th char
	## BWF - part of LanguageRecord interface. may be a common base class method
	def ReduceTextList(self, damIdList):
		damIdSet = set()
		for (damId, index, status, fieldName) in damIdList:
			damIdOut = LanguageRecordInterface.transformToTextId(damId)
			damIdSet.add((damIdOut, index, status))
		return damIdSet


	def AltName(self):
		result = self.record.get("AltName")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result
	def AltNameList(self):
		altNametext = self.AltName()

		if altNametext != None:
			altNamelist = altNametext.split("|")
			listStripped = map(lambda altName: altName.strip(), altNamelist)
			return list(listStripped)

		return []

	def APIDevAudio(self):
		return self.record.get("APIDevAudio")

	def APIDevText(self):
		return self.record.get("APIDevText")

	def APIDevVideo(self):
		return self.record.get("APIDevVideo")

	# deprecated
	def CoLicensor(self):
		coLicensorList = self.CoLicensorList()

		if coLicensorList != None and len(coLicensorList) != 0:
			return coLicensorList[0]

		return None

	def CoLicensorList(self):
		return self.SplitText(self.record.get("CoLicensor"))

	def HasCoLicensor(self, coLicensor):
		coLicensors = self.CoLicensorList()
		return coLicensors != None and coLicensor in coLicensors

	def Copyrightc(self):
		return self.record.get("Copyrightc")

	def HasPublicDomainCopyrightc(self):
		"""
		Check if Copyrightc contains the phrase 'Public Domain' (case-insensitive).

		Returns:
		bool: True if 'Public Domain' is found in Copyrightc, False otherwise.
		"""
		# Compile the regular expression for 'Public Domain' with case-insensitivity
		pattern = re.compile(r"Public Domain", re.IGNORECASE)

		return self.Copyrightc() != None and pattern.search(self.Copyrightc())

	def Copyrightp(self):
		return self.record.get("Copyrightp")

	def Copyright_Video(self):
		return self.record.get("Copyright_Video")

	def Country(self):
		return self.record.get("Country")

	def CountryAdditional(self):
		return self.record.get("CountryAdditional")

	def CreativeCommonsAudio(self):
		return self.record.get("CreativeCommonsAudio")

	def CreativeCommonsAudioWaiver(self):
		return self.record.get("CreativeCommonsAudioWaiver")

	def CreativeCommonsText(self):
		return self.record.get("CreativeCommonsText")

	def DBL_Load_Notes(self):
		return self.record.get("DBL_Load_Notes")

	def DBL_Load_Status(self):
		return self.record.get("DBL_Load_Status")

	def DBPAudio(self):
		return self.record.get("DBPAudio")

	def DBPDate(self, index):
		if index == 1:
			return self.record.get("DBPDate") or self.record.get("DBPDate1")
		elif index == 2:
			return self.record.get("DBPDate2")
		elif index == 3:
			return self.record.get("DBPDate")
		else:
			return None

	def DBPFont(self, index):
		if index == 1:
			return self.record.get("DBPFont") or self.record.get("DBPFont1")
		elif index == 2:
			return self.record.get("DBPFont2")
		elif index == 3:
			return self.record.get("DBPFont3")
		else:
			print("ERROR: DBFont index must be 1, 2, or 3.")
			sys.exit()

	def DBPMobile(self):
		return self.record.get("DBPMobile")

	def DBPText(self):
		return self.record.get("DBPText")

	def DBPTextOT(self):
		return self.record.get("DBPTextOT")

	def DBPWebHub(self):
		return self.record.get("DBPWebHub")

	def DBP_Equivalent(self):
		result = self.record.get("DBP_Equivalent")
		if result in {"N/A", "#N/A"}:
			return None
		else:
			return result

	def DBP_Equivalent2(self):
		result = self.record.get("DBP_Equivalent2")
		if result in {"N/A", "#N/A"}:
			return None
		else:
			return result

	def DBP_Equivalent3(self):
		result = self.record.get("DBP_Equivalent3")
		if result in {"N/A", "#N/A"}:
			return None
		else:
			return result

	def DBP_EquivalentByIndex(self, index):
		if index == 1:
			return self.DBP_Equivalent()
		elif index == 2:
			return self.DBP_Equivalent2()
		elif index == 3:
			return self.DBP_Equivalent3()
		else:
			print("ERROR: DBP_Equivalent index must be 1, 2, or 3.")
			sys.exit()

	def DBP_EquivalentSet(self):
		if self.DBP_Equivalent() != None:
			return self.DBP_Equivalent()
		elif self.DBP_Equivalent2 != None:
			return self.DBP_Equivalent2()
		elif self.DBP_Equivalent3 != None:
			return self.DBP_Equivalent3()
		else:
			None

	def Download(self):
		return self.record.get("Download")

	def ElectronicPublisher(self, index):
		if index == 1:
			return self.record.get("ElectronicPublisher1")
		elif index == 2:
			return self.record.get("ElectronicPublisher2")
		elif index == 3:
			return self.record.get("ElectronicPublisher3")
		else:
			return None

	def ElectronicPublisherWebsite(self, index):
		if index == 1:
			return self.record.get("ElectronicPublisherWebsite1")
		elif index == 2:
			return self.record.get("ElectronicPublisherWebsite2")
		elif index == 3:
			return self.record.get("ElectronicPublisherWebsite3")
		else:
			return None

	def EthName(self):
		return self.record.get("EthName").strip() if self.record.get("EthName") != None else None 

	def FairUseLimit(self):
		return self.record.get("FairUseLimit")

	def FairUseLimitValue(self):
		return self.record.get("FairUseLimitValue")

	def Gideon_Audio_Excluded(self):
		return self.record.get("Gideon_Audio_Excluded")

	def Gideon_Text_Excluded(self):
		return self.record.get("Gideon_Text_Excluded")

	def HeartName(self):
		return self.record.get("HeartName").strip() if self.record.get("HeartName") != None else None

	def HubText(self):
		return self.record.get("HubText")

	def ISO(self):
		return self.record.get("ISO")

	def ItunesPodcast(self):
		return self.record.get("ItunesPodcast")

	def LangName(self):
		return self.record.get("LangName")

	def SplitText(self, input_string):
		if input_string == None or input_string == "" or input_string.strip() == None:
			return None

		# Split the string by semicolon and strip whitespace from each item
		result = [item.strip() for item in input_string.split(";") if item.strip()]

		if not result:
			return None

		return result

	# deprecated
	def Licensor(self):
		licensorList = self.LicensorList()

		if licensorList != None and len(licensorList) != 0:
			return licensorList[0]

		return None

	def LicensorList(self):
		return self.SplitText(self.record.get("Licensor"))

	def HasLicensor(self, licensor):
		licensors = self.LicensorList()
		return licensors != None and licensor in licensors

	def HasPublicDomainLicensor(self):
		"""
		Check if any licensor in the list contains the phrase 'Public Domain' (case-insensitive).

		Args:
		licensors (list of str): A list containing strings of licensor names.

		Returns:
		bool: True if 'Public Domain' is found in any string, False otherwise.
		"""
		# Compile the regular expression for 'Public Domain' with case-insensitivity
		pattern = re.compile(r"Public Domain", re.IGNORECASE)

		licensors = self.LicensorList()

		if licensors != None:
			for licensor in licensors:
				if pattern.search(licensor):
					return True

		# Return False if 'Public Domain' was not found in any of the licensors
		return False

	def MobileText(self):
		return self.record.get("MobileText")

	def MobileVideo(self):
		return self.record.get("MobileVideo")

	def ND_DBL_Load_Notes(self):
		return self.record.get("ND_DBL_Load_Notes")

	def ND_HUBLink(self, index):
		if index == 1:
			return self.record.get("ND_HUBLink") or self.record.get("ND_HUBLink1")
		elif index == 2:
			return self.record.get("ND_HUBLink2")
		elif index == 3:
			return self.record.get("ND_HUBLink3")
		else:
			return None

	def ND_Recording_Status(self):
		return self.record.get("ND_Recording_Status")

	def ND_StockNumber(self):
		result = self.record.get("ND_StockNumber")
		result = result.strip()

		if result == "none":
			result = None

		return result

	def NTAudioDamLoad(self):
		return self.record.get("NTAudioDamLoad")

	def NTOrder(self):
		result = self.record.get("NTOrder")
		if result == None or result == "NA":
			return "Traditional"
		else:
			return result

	def Numerals(self):
		return self.record.get("Numerals")

	def Orthography(self, index):
		if index == 1:
			return self.record.get("_x0031_Orthography")
		elif index == 2:
			return self.record.get("_x0032_Orthography")
		elif index == 3:
			return self.record.get("_x0033_Orthography")
		else:
			print("ERROR: Orthography index must be 1, 2, or 3, but was %s" % str(index,))
			sys.exit()

	def OTOrder(self):
		result = self.record.get("OTOrder")
		if result == None or result == "NA":
			return "Traditional"
		else:
			return result

	def Portion(self):
		return self.record.get("Portion")

	def PostedToServer(self):
		return self.record.get("PostedToServer")

	def Reg_HUBLink(self, index):
		if index == 1:
			return self.record.get("Reg_HUBLink1")
		elif index == 2:
			return self.record.get("Reg_HUBLink2")
		elif index == 3:
			return self.record.get("Reg_HUBLink3")
		else:
			print("ERROR: Reg_HUBLink index must be 1, 2, 3")
			sys.exit()

	def HasTexOnlyRecordingStatus(self):
		return self.Reg_Recording_Status() == "Text Only"

	def Reg_Recording_Status(self):
		return self.record.get("Reg_Recording_Status")

	def IsActive(self):
		return self.Reg_Recording_Status() != "Discontinued" and self.Reg_Recording_Status() != "Inactive" and self.Reg_Recording_Status() != "Void"

	def Reg_StockNumber(self):
		return self.record.get("Reg_StockNumber")

	def StockNumberByFilesetIdAndType(self, filesetId, typeCode):
		damIds = self.DamIdList(typeCode)
		for (damId, _, _, fieldname) in damIds:
			if filesetId == damId:
				if LanguageRecord.ND_StockNumberDamIdIndex.get(fieldname) == True:
					return self.ND_StockNumber()
				elif LanguageRecord.Reg_StockNumberDamIdIndex.get(fieldname) == True:
					return self.Reg_StockNumber()
		return None

	def StockNumberByFilesetId(self, filesetId):
		for typeCode in {"audio", "text", "video"}:
			stockNumber = self.StockNumberByFilesetIdAndType(
				LanguageRecordInterface.GetFilesetIdLen10(filesetId),
				typeCode
			)
			if stockNumber != None:
				return stockNumber
		return None

	def Restrictions(self):
		return self.record.get("Restrictions")

	def Selection(self):
		return self.record.get("Selection")

	def Streaming(self):
		return self.record.get("Streaming")

	def USX_Date(self, index):
		if index == 1:
			return self.record.get("USX_Date") or self.record.get("USX_Date1")
		elif index == 2:
			return self.record.get("USX_Date2")
		elif index == 3:
			return self.record.get("USX_Date3")
		else:
			return None

	def Version(self):
		return self.record.get("Version")

	def Video_John_DamLoad(self):
		return self.record.get("Video_John_DamLoad")

	def Video_Luke_DamLoad(self):
		return self.record.get("Video_Luke_DamLoad")

	def Video_Mark_DamLoad(self):
		return self.record.get("Video_Mark_DamLoad")

	def Video_Matt_DamLoad(self):
		return self.record.get("Video_Matt_DamLoad")

	def LORA(self):
		return int(self.record.get("LORA"))

	def Other_Agreement(self):
		return int(self.record.get("OtherAgreement"))

	def Text_Agreement(self):
		return int(self.record.get("TextAgreement"))

	def TraditionalRecording(self):
		return int(self.record.get("TraditionalRecording"))

	def VirtualRecording(self):
		return int(self.record.get("VirtualRecording"))

	def Partner(self):
		return int(self.record.get("Partner"))

	def Joint(self):
		return int(self.record.get("Joint"))

	def HearThis(self):
		return int(self.record.get("HearThis"))

	def Render(self):
		return int(self.record.get("Render"))

	def HasTraditionalRecording(self):
		return self.TraditionalRecording() == 1

	def HasVirtualRecording(self):
		return self.VirtualRecording() == 1

	def HasPartner(self):
		return self.Partner() == 1

	def HasJoint(self):
		return self.Joint() == 1

	def HasHearThis(self):
		return self.HearThis() == 1

	def HasRender(self):
		return self.Render() == 1

	def Has_Lora_Agreement(self):
		return self.LORA() == 1

	def Has_Other_Agreement(self):
		return self.Other_Agreement() == 1

	def Has_Text_Agreement(self):
		return self.Text_Agreement() == 1

	def Has_Ambiguous_Agreement(self):
		total = self.LORA() + self.Text_Agreement() + self.Other_Agreement()
		return total > 1

	def Has_Ambiguous_Methodology(self):
		total = self.TraditionalRecording() + self.VirtualRecording() + self.Partner() + self.Joint() + self.HearThis() + self.Render()
		return total > 1

	def Volumne_Name(self):
		result = self.record.get("Volumne_Name")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result

	def Has_Copyright_Video(self):
		return self.Copyright_Video() != None

	def WebHubVideo(self):
		return self.record.get("WebHubVideo")

"""
*********************************************************
*** Everything below here is intended for unit test only.
*********************************************************
"""

"""
# Get listing of damIds, per stock no
if __name__ == '__main__':
	config = Config.shared()
	reader = LanguageReaderCreator().create(config)
	for languageRecord in reader.resultSet:
		stockNo = languageRecord.Reg_StockNumber()
		for typeCode in ["audio", "text", "video"]:
			mediaDamIds = []
			indexes = [1] if typeCode == "video" else [1,2,3]
			for index in indexes:
				damIds = languageRecord.DamIdMap(typeCode, index)
				if len(damIds) > 0:
					print(stockNo, typeCode, index, damIds.keys())
"""

"""
# Get alphabetic list distinct field names
if (__name__ == '__main__'):
	fieldCount = {}
	config = Config()
	reader = LPTSExtractReader(config)
	for languageRecord in reader.resultSet:
		record = languageRecord.record
		for fieldName in record.keys():
			#print(fieldName)
			count = fieldCount.get(fieldName, 0)
			count += 1
			fieldCount[fieldName] = count
	for fieldName in sorted(fieldCount.keys()):
		count = fieldCount[fieldName]
		print("%s,%s" % (fieldName, count))
"""

"""
if (__name__ == '__main__'):
	config = Config()
	reader = LPTSExtractReader(config)
	reader.getFieldNames()
"""
"""
if (__name__ == '__main__'):
	config = Config()
	reader = LPTSExtractReader(config)
	for rec in reader.resultSet:
		for index in [1, 2, 3]:
			print(rec.Reg_StockNumber(), index)
			#prefixSet = set()
			for typeCode in ["text", "audio", "video"]:
				if typeCode != "video" or index == 1:
					damidSet = rec.DamIds2(typeCode, index)
					if len(damidSet) > 0:
						print(typeCode, index, damidSet)
						#for damid in damidSet:
						#	prefixSet.add(damid[:6])
			#if len(prefixSet) > 1:
			#	print("ERROR: More than one DamId prefix in set %s" % (",".join(prefixSet)))
"""
"""
# Find bible_ids that exist in multiple stock numbers
# Find stock numbers that contain multiple bible_ids
if (__name__ == '__main__'):
	bibleIdMap = {}
	config = Config()
	reader = LPTSExtractReader(config)
	for rec in reader.resultSet:
		#print(rec.Reg_StockNumber(), rec.DBP_Equivalent(), rec.DBP_Equivalent2())
		#if len(rec.Reg_StockNumber()) != 9:
		#	print("Different Stocknumber", rec.Reg_StockNumber())
		stockNo = rec.Reg_StockNumber()
		bibleId1 = rec.DBP_Equivalent()
		bibleId2 = rec.DBP_Equivalent2()
		#if bibleId2 != None:
		#	print("StockNo:", stockNo, "BibleId 1:", bibleId1, "BibleId 2:", bibleId2)
		#if bibleId1 in bibleIdMap.keys():
		#	priorStockNum = bibleIdMap[bibleId1]
		#	print("StockNo:", stockNo, "StockNo:", priorStockNum, "BibleId1:", bibleId1)
		if bibleId2 in bibleIdMap.keys():
			priorStockNum = bibleIdMap[bibleId2]
			print("StockNo:", stockNo, "StockNo:", priorStockNum, "BibleId2:", bibleId2)
		if bibleId1 != None:
			bibleIdMap[bibleId1] = stockNo
		if bibleId2 != None:
			bibleIdMap[bibleId2] = stockNo
"""


if (__name__ == '__main__'):
	from Config import *
	from LanguageReaderCreator import *	
	result = {}
	config = Config()
	reader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	stockNumber = None
	if len(sys.argv) > 2:
		stockNumber = sys.argv[2]

	for rec in reader.resultSet:
		textDamIds = rec.DamIdList("text")
		reduceTextDamIds = rec.ReduceTextList(textDamIds)
		#if len(textDamIds) > 1:
		#	print(textDamIds)
		listDamIds = list(reduceTextDamIds)
		if len(listDamIds) > 1 and listDamIds[0][0][:6] != listDamIds[1][0][:6]:
			print(rec.Reg_StockNumber(), listDamIds)

		if stockNumber == rec.Reg_StockNumber() or stockNumber == rec.ND_StockNumber():
			audioDamIds = rec.DamIdList("audio")
			print("")
			print("=========================================", stockNumber, "=========================================")
			print("Reg_StockNumber:", rec.Reg_StockNumber())
			print("ND_StockNumber:", rec.ND_StockNumber())
			print("Text DamIds:", textDamIds)
			print("Audio DamIds:", audioDamIds)
			print("ReduceText DamIds:", reduceTextDamIds)
			print("=========================================", stockNumber, "=========================================")
			print("")

