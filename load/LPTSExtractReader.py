# LPTSExtractReader
#
# This program reads the LPTS Extract and produces hash tables.
#

import io
import sys
import os
from xml.dom import minidom
from Config import *
from Log import *

class LPTSExtractReader:

	def __init__(self, config):
		self.resultSet = []
		dom = None
		try:
			doc = minidom.parse(config.filename_lpts_xml)
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
								#print(fldNode.nodeName + " = " + fldNode.firstChild.nodeValue)
								resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue

						self.resultSet.append(LPTSRecord(resultRow))
		self.bibleIdMap = self.getBibleIdMap()
		self.filesetIdMap = None
		print("LPTS Extract with %d records and %d BibleIds is loaded." % (len(self.resultSet), len(self.bibleIdMap.keys())))


    ## Generates Map bibleId: [(index, LPTSRecord)], called by class init
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


	## Generates a Map Records where stock number is the Key {stockNum: LPTSRecord}
	def getStockNumberMap(self):
		stockNumMap = {}
		for rec in self.resultSet:
			stockNum = rec.Reg_StockNumber()
			if stockNum != None:
				if stockNumMap.get(stockNum) == None:
					stockNumMap[stockNum] = rec
				else:
					print("ERROR: Duplicate Stock Num", stockNum);
			else:
				print("ERROR: Record has no stock number")
		return stockNumMap



	## Returns one (record, index) for typeCode, bibleId, filesetId
	## This is a strict method that only returns when BibleId matches and status is Live
	def getLPTSRecord(self, typeCode, bibleId, filesetId):
		normFilesetId = filesetId[:10]
		lptsRecords = self.bibleIdMap.get(bibleId)
		if lptsRecords != None:
			for (index, record) in lptsRecords:
				damIdSet = record.DamIds(typeCode, index)
				if normFilesetId in damIdSet:
					return (record, index)
		return (None, None)


	## This method is a hack, because in LPTS a text damId is 10 char, 
	## but in DBP it is 6 char, when searching LPTS the same text damid
	## can be found in multiple record, but there is no way to know
	## which is correct.
	def getLPTSRecordLoose(self, typeCode, bibleId, filesetId):
		result = self.getLPTSRecord(typeCode, bibleId, filesetId)
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
	## Otherwise it is exactly like getLPTSRecord
	## Return [(record, index)]
	def getLPTSRecordsAll(self, typeCode, bibleId, filesetId):
		results = []
		normFilesetId = filesetId[:10]
		lptsRecords = self.bibleIdMap.get(bibleId)
		if lptsRecords != None:
			for (index, record) in lptsRecords:
				damIdList = record.DamIdMap(typeCode, index)
				if normFilesetId in damIdList:
					#return(record, index)
					results.append((record, index))
		#return (None, None)
		return results		


	## This is a more permissive way to get LPTS Records, it does not require
	## a type or bibleId.  So, it can only be used for non-index fields
	## It returns an array of statuses and records, i.e. [(status, lptsRecord)]
	def getFilesetRecords(self, filesetId):
		if self.filesetIdMap == None:
			self.filesetIdMap = {}
			damIdDict = {**LPTSRecord.audio1DamIdDict,
						**LPTSRecord.audio2DamIdDict,
						**LPTSRecord.audio3DamIdDict,
						**LPTSRecord.text1DamIdDict,
						**LPTSRecord.text2DamIdDict,
						**LPTSRecord.text3DamIdDict,
						**LPTSRecord.videoDamIdDict}
			for lptsRecord in self.resultSet:
				record = lptsRecord.record
				hasKeys = set(damIdDict.keys()).intersection(set(record.keys()))
				for key in hasKeys:
					statusKey = damIdDict[key]
					damId = record[key]
					if "Text" in key:
						damId = damId[:6]
					status = record.get(statusKey)
					statuses = self.filesetIdMap.get(damId, [])
					statuses.append((status, lptsRecord))
					self.filesetIdMap[damId] = statuses
		return self.filesetIdMap.get(filesetId[:10], None)


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


class LPTSRecord:

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
			if status in {"Live", "live"}:
				results.add(damId)
		return results

	## Return a map of damId: status of the damIds in a record
	def DamIdMap(self, typeCode, index):
		if not typeCode in {"audio", "text", "video"}:
			print("ERROR: Unknown typeCode '%s', audio, text, or video is expected." % (typeCode))
		if not index in {1, 2, 3}:
			print("ERROR: Unknown DamId index '%s', 1,2, or 3 expected." % (index))
			sys.exit()
		if typeCode == "audio":
			if index == 1:
				damIdDict = LPTSRecord.audio1DamIdDict
			elif index == 2:
				damIdDict = LPTSRecord.audio2DamIdDict
			elif index == 3:
				damIdDict = LPTSRecord.audio3DamIdDict
		elif typeCode == "text":
			if index == 1:
				damIdDict = LPTSRecord.text1DamIdDict
			elif index == 2:
				damIdDict = LPTSRecord.text2DamIdDict
			elif index == 3:
				damIdDict = LPTSRecord.text3DamIdDict
		elif typeCode == "video":
			damIdDict = LPTSRecord.videoDamIdDict
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
		return results

	## This is for text only, and is identical to DamIdMap, except it returns 10 characters
	def TextDamIdMap(self, index):
		if not index in {1, 2, 3}:
			print("ERROR: Unknown DamId index '%s', 1,2, or 3 expected." % (index))
			sys.exit()
		if index == 1:
			damIdDict = LPTSRecord.text1DamIdDict
		elif index == 2:
			damIdDict = LPTSRecord.text2DamIdDict
		elif index == 3:
			damIdDict = LPTSRecord.text3DamIdDict
		hasKeys = set(damIdDict.keys()).intersection(set(self.record.keys()))
		results = {}
		for key in hasKeys:
			statusKey = damIdDict[key]
			damId = self.record[key]
			status = self.record.get(statusKey)
			results[damId] = status
		return results

	def AltName(self):
		result = self.record.get("AltName")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result

	def APIDevAudio(self):
		return self.record.get("APIDevAudio")

	def APIDevText(self):
		return self.record.get("APIDevText")

	def APIDevVideo(self):
		return self.record.get("APIDevVideo")

	def CoLicensor(self):
		return self.record.get("CoLicensor")

	def Copyrightc(self):
		return self.record.get("Copyrightc")

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
		return self.record.get("EthName")

	def FairUseLimit(self):
		return self.record.get("FairUseLimit")

	def FairUseLimitValue(self):
		return self.record.get("FairUseLimitValue")

	def Gideon_Audio_Excluded(self):
		return self.record.get("Gideon_Audio_Excluded")

	def Gideon_Text_Excluded(self):
		return self.record.get("Gideon_Text_Excluded")

	def HeartName(self):
		return self.record.get("HeartName")

	def HubText(self):
		return self.record.get("HubText")

	def ISO(self):
		return self.record.get("ISO")

	def ItunesPodcast(self):
		return self.record.get("ItunesPodcast")

	def LangName(self):
		return self.record.get("LangName")

	def Licensor(self):
		return self.record.get("Licensor")

	def MobileText(self):
		return self.record.get("MobileText")

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
		return self.record.get("ND_StockNumber")

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
			Log.fatalError("Orthography index must be 1, 2, or 3, but was %s" % str(index,))

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

	def Reg_Recording_Status(self):
		return self.record.get("Reg_Recording_Status")

	def Reg_StockNumber(self):
		return self.record.get("Reg_StockNumber")

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

	def Volumne_Name(self):
		result = self.record.get("Volumne_Name")
		if result == "N/A" or result == "#N/A":
			return None
		else:
			return result

	def WebHubVideo(self):
		return self.record.get("WebHubVideo")

"""
if (__name__ == '__main__'):
	config = Config()
	reader = LPTSExtractReader(config)
	stockNumMap = reader.getStockNumberMap()
	for stockNum in sorted(stockNumMap.keys()):
		record = stockNumMap[stockNum]
		print(stockNum)
		for index in [1, 2, 3]:
			damIdMap = record.TextDamIdMap(index)
			for (damId, status) in damIdMap.items():
				bibleId = record.DBP_EquivalentByIndex(index)
				print('\t', index, bibleId, damId, status)
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
