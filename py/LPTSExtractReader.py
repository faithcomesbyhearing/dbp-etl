# LPTSExtractReader
#
# This program reads the LPTS Extract and produces hash tables.
#

import io
import sys
import os
from xml.dom import minidom
from Config import *

class LPTSExtractReader:

	def __init__(self, config):
		self.resultSet = []
		doc = minidom.parse(config.filename_lpts_xml)
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
								#print(fldNode.nodeName + " = " + fldNode.firstChild.nodeValue)
								resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue

						self.resultSet.append(LPTSRecord(resultRow))
		print(len(self.resultSet), " LPTS Records.")


    ## StockNum is the offical unique identifier of LPTS
	def getStockNumMap(self):
		stockNumMap = {}
		for rec in self.resultSet:
			stockNum = rec.Reg_StockNumber()
			stockNumMap[stockNum] = rec
		return stockNumMap


    ## Generates Map bibleId: [LPTSRecord]
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


#	## Generated Map filesetId: [LPTSRecord]
#	def getFilesetIdMap(self):
#		filesetIdMap = {}
#		for rec in self.resultSet:
#			filesetIds = rec.DamIds()
#			for (filesetId, statuses) in filesetIds.items():
#				filesetRecords = filesetIdMap.get(filesetId, [])
#				filesetRecords.append(rec)
#				filesetIdMap[filesetId] = filesetRecords
#		return filesetIdMap


#	def checkUniqueNames(self):
#		self.checkUniqueName("DBP_Equivalent")


	def checkUniqueName(self, name):
		occurrances = {}
		for rec in self.resultSet:
			value = rec.record.get(name)
			if not value in {None, "", "N/A", "#N/A", "NA"}:
				stockList = occurrances.get(value, [])
				stockList.append(rec.Reg_StockNumber())
				occurrances[value] = stockList
		results = {}
		for name, stockNums in occurrances.items():
			if len(stockNums) > 1:
				results[name] = stockNums
				print(name, "->", stockNums)
		return results

	## deprecated, replaced by getFilesetIdMap
	def getAudioMap(self):
		audioDic = {
			"CAudioDAMID1": "CAudioStatus1",
			"CAudioDamStockNo": "CAudioDamStatus",
			"ND_NTAudioDamID1": "ND_NTAudioDamIDStatus1",
			"ND_NTAudioDamID2": "ND_NTAudioDamIDStatus2",
			"ND_OTAudioDamID1": "ND_OTAudioDamIDStatus1",
			"ND_OTAudioDamID2": "ND_OTAudioDamIDStatus2",
			"Reg_NTAudioDamID1": "Reg_NTAudioDamIDStatus1",
			"Reg_NTAudioDamID2": "Reg_NTAudioDamIDStatus2",
			"Reg_OTAudioDamID1": "Reg_OTAudioDamIDStatus1",
			"Reg_OTAudioDamID2": "Reg_OTAudioDamIDStatus2"}
		return self.isInMap(audioDic)

	## deprecated, replaced by getFilesetIdMap
	def getTextMap(self):
		textDic = {
			"ND_NTTextDamID1": "ND_NTTextDamIDStatus1",
			"ND_NTTextDamID2": "ND_NTTextDamIDStatus2", 
			"ND_NTTextDamID3": "ND_NTTextDamIDStatus3",
			"ND_OTTextDamID1": "ND_OTTextDamIDStatus1",
			"ND_OTTextDamID2": "ND_OTTextDamIDStatus2", 
			"ND_OTTextDamID3": "ND_OTTextDamIDStatus3",
			"Reg_NTTextDamID1": "Reg_NTTextDamIDStatus1",
			"Reg_NTTextDamID2": "Reg_NTTextDamIDStatus2",
			"Reg_NTTextDamID3": "Reg_NTTextDamIDStatus3", 
			"Reg_OTTextDamID1": "Reg_OTTextDamIDStatus1", 
			"Reg_OTTextDamID2": "Reg_OTTextDamIDStatus2",  
			"Reg_OTTextDamID3": "Reg_OTTextDamIDStatus3"}
		resultMap = {}
		for rec in self.resultSet:
			row = rec.record # get hashtable inside record
			for damIdKey in textDic.keys():
				if damIdKey in row:
					damId = row[damIdKey]
					statusName = textDic[damIdKey]
					#rec.record["THIS_DAMID"] = damId
					#rec.record["THIS_DAMID_STATUS"] = row.get(statusName)
					resultMap[damId[:6]] = rec # Truncated to 6 chars to match bucket
		return resultMap
	
	## deprecated, replaced by getFilesetIdMap
	def getVideoMap(self):
		videoDic = {
			"Video_John_DamStockNo": "Video_John_DamStatus",
			"Video_Luke_DamStockNo": "Video_Luke_DamStatus",
			"Video_Mark_DamStockNo": "Video_Mark_DamStatus",
			"Video_Matt_DamStockNo": "Video_Matt_DamStatus"}
		return self.isInMap(videoDic)

	## deprecated, replaced by getFilesetIdMap
	def getAllFilesetMap(self):
		hashMap = {}
		audioMap = self.getAudioMap()
		for key in audioMap.keys():
			hashMap[key] = audioMap[key]

		textMap = self.getTextMap()
		for key in textMap.keys():
			hashMap[key] = textMap[key]

		videoMap = self.getVideoMap()
		for key in videoMap.keys():
			hashMap[key] = videoMap[key]
			
		return hashMap


	## deprecated, replaced by LPTSRecord.DamIds()
	def isInMap(self, damIdMap):
		resultMap = {}
		for rec in self.resultSet:
			row = rec.record # get hashtable inside record
			for damIdKey in damIdMap.keys():
				if damIdKey in row:
					damId = row[damIdKey]
					statusName = damIdMap[damIdKey]
					#rec.record["THIS_DAMID"] = damId
					#rec.record["THIS_DAMID_STATUS"] = row.get(statusName)
					resultMap[damId] = rec
		return resultMap	


#
# The LPTSRecord matches one record in the Extract, and provides a type safe way
# to access it in order to prevent bugs by spelling errors, and to perform
# data cleanup.
# 

class LPTSRecord:

	def __init__(self, record):
		self.record = record

	def recordLen(self):
		return len(self.record.keys())

	# Deprecated.  This does not work, because there on one record, and many damId keys point to it.
	# This status is just the status of the last key added.
	# This field was added to record when indexed by damId.  It is the status associated with that damId
#	def thisDamIdStatus(self):
#		return self.record.get("THIS_DAMID_STATUS")

	## Return the damId's of a record in a map of damId keys
	## result = [(statusKey, status)]
	def DamIds(self, index):
		damIdDict1 = {
			"CAudioDAMID1": "CAudioStatus1",
			"CAudioDamStockNo": 	"CAudioDamStatus",
			"ND_NTAudioDamID1": 	"ND_NTAudioDamIDStatus1",
			"ND_OTAudioDamID1": 	"ND_OTAudioDamIDStatus1",
			"Reg_NTAudioDamID1": 	"Reg_NTAudioDamIDStatus1",
			"Reg_OTAudioDamID1": 	"Reg_OTAudioDamIDStatus1",
			"ND_NTTextDamID1": 		"ND_NTTextDamIDStatus1",
			"ND_OTTextDamID1": 		"ND_OTTextDamIDStatus1",
			"Reg_NTTextDamID1": 	"Reg_NTTextDamIDStatus1",
			"Reg_OTTextDamID1": 	"Reg_OTTextDamIDStatus1", 
			"Video_John_DamStockNo": "Video_John_DamStatus",
			"Video_Luke_DamStockNo": "Video_Luke_DamStatus",
			"Video_Mark_DamStockNo": "Video_Mark_DamStatus",
			"Video_Matt_DamStockNo": "Video_Matt_DamStatus"}
		damIdDict2 = {
			"ND_CAudioDamID2": 		"ND_CAudioDamIDStatus2",
			"ND_NTAudioDamID2": 	"ND_NTAudioDamIDStatus2",
			"ND_OTAudioDamID2": 	"ND_OTAudioDamIDStatus2",
			"Reg_CAudioDamID2": 	"Reg_CAudioDamIDStatus2",
			"Reg_NTAudioDamID2": 	"Reg_NTAudioDamIDStatus2",
			"Reg_OTAudioDamID2": 	"Reg_OTAudioDamIDStatus2",
			"ND_NTTextDamID2": 		"ND_NTTextDamIDStatus2",
			"ND_OTTextDamID2": 		"ND_OTTextDamIDStatus2", 
			"Reg_NTTextDamID2": 	"Reg_NTTextDamIDStatus2",
			"Reg_OTTextDamID2": 	"Reg_OTTextDamIDStatus2"}
		damIdDict3 = {
			"ND_CAudioDamID3": 		"ND_CAudioDamIDStatus3",
			"ND_NTAudioDamID3": 	"ND_NTAudioDamIDStatus3",
			"ND_OTAudioDamID3": 	"ND_OTAudioDamIDStatus3",
			"Reg_CAudioDamID3": 	"Reg_CAudioDamIDStatus3",
			"Reg_NTAudioDamID3":	"Reg_NTAudioDamIDStatus3",
			"Reg_OTAudioDamID3":	"Reg_OTAudioDamIDStatus3",
			"ND_NTTextDamID3": 		"ND_NTTextDamIDStatus3",
			"ND_OTTextDamID3": 		"ND_OTTextDamIDStatus3",
			"Reg_NTTextDamID3": 	"Reg_NTTextDamIDStatus3", 
			"Reg_OTTextDamID3": 	"Reg_OTTextDamIDStatus3"}
		if index == 1:
			damIdDict = damIdDict1
		elif index == 2:
			damIdDict = damIdDict2
		elif index == 3:
			damIdDict = damIdDict3
		else:
			print("ERROR: Unknown DamId index '%s', 1,2, or 3 expected." % (index))
			sys.exit()
		hasKeys = set(damIdDict.keys()).intersection(set(self.record.keys()))
		results = {}
		for key in hasKeys:
			#print("key", key)
			statusKey = damIdDict[key]
			#print("statusKey", statusKey)
			damId = self.record[key]
			#print("damId", damId)
			status = self.record.get(statusKey)
			statuses = results.get(damId, [])
			statuses.append((statusKey, status))
			results[damId] = statuses
		return results

	def LangName(self):
		return self.record.get("LangName")

	def Reg_StockNumber(self):
		return self.record.get("Reg_StockNumber")

	def Reg_Recording_Status(self):
		return self.record.get("Reg_Recording_Status")

	def ND_StockNumber(self):
		return self.record.get("ND_StockNumber")

	def HeartName(self):
		return self.record.get("HeartName")

	def ISO(self):
		return self.record.get("ISO")

	def Licensor(self):
		return self.record.get("Licensor")

	def Copyrightc(self):
		return self.record.get("Copyrightc")

	def Copyrightp(self):
		return self.record.get("Copyrightp")

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

	def Volumne_Name(self):
		result = self.record.get("Volumne_Name")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result

	def Country(self):
		return self.record.get("Country")

	def DBPText(self):
		return self.record.get("DBPText")

	def DBPTextOT(self):
		return self.record.get("DBPTextOT")

	def DBPAudio(self):
		return self.record.get("DBPAudio")

	def DBPMobile(self):
		return self.record.get("DBPMobile")

	def DBPWebHub(self):
		return self.record.get("DBPWebHub")

	def Download(self):
		return self.record.get("Download")

	def Streaming(self):
		return self.record.get("Streaming")

	def MobileText(self):
		return self.record.get("MobileText")

	def HubText(self):
		return self.record.get("HubText")

	def APIDevText(self):
		return self.record.get("APIDevText")

	def APIDevAudio(self):
		return self.record.get("APIDevAudio")

	def FairUseLimitValue(self):
		return self.record.get("FairUseLimitValue")

	def FairUseLimit(self):
		return self.record.get("FairUseLimit")

	def EthName(self):
		return self.record.get("EthName")

	def AltName(self):
		result = self.record.get("AltName")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result

	def CreativeCommonsText(self):
		return self.record.get("CreativeCommonsText")

	def CreativeCommonsAudioWaiver(self):
		return self.record.get("CreativeCommonsAudioWaiver")

	def ItunesPodcast(self):
		return self.record.get("ItunesPodcast")
	
	def WebHubVideo(self):
		return self.record.get("WebHubVideo")

	def Copyright_Video(self):
		return self.record.get("Copyright_Video")

	def Orthography(self, index):
		if index == 1:
			return self.record.get("_x0031_Orthography")
		elif index == 2:
			return self.record.get("_x0032_Orthography")
		elif index == 3:
			return self.record.get("_x0033_Orthography")
		else:
			print("ERROR: Orthography index must be 1, 2, or 3.")
			sys.exit()

	def DBPFont(self, index):
		if index == 1:
			return self.record.get("DBPFont")
		elif index == 2:
			return self.record.get("DBPFont2")
		elif index == 3:
			return self.record.get("DBPFont3")
		else:
			print("ERROR: DBFont index must be 1, 2, or 3.")
			sys.exit()

	def USX_Date1(self):
		return self.record.get("USX_Date1")

	def PostedToServer(self):
		return self.record.get("PostedToServer")

	def DBPDate(self):
		return self.record.get("DBPDate")

	def DBPDate2(self):
		return self.record.get("DBPDate2")

	def Version(self):
		return self.record.get("Version")

	def APIDevVideo(self):
		return self.record.get("APIDevVideo")

	def NTOrder(self):
		result = self.record.get("NTOrder")
		if result == None or result == "NA":
			return "Traditional"
		else:
			return result

	def OTOrder(self):
		result = self.record.get("OTOrder")
		if result == None or result == "NA":
			return "Traditional"
		else:
			return result









