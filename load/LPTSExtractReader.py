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
		self.bibleIdMap = self.getBibleIdMap()
		print(len(self.bibleIdMap.keys()), " BibleIds found.")


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


	## Returns one record for bibleId, filesetId pair
	def getLPTSRecord(self, typeCode, bibleId, filesetId):
		normFilesetId = filesetId[:10]
		lptsRecords = self.bibleIdMap.get(bibleId)
		if lptsRecords != None:
			for (index, record) in lptsRecords:
				damIdMap = record.DamIds(typeCode, index)
				(statusKey, status) = damIdMap.get(normFilesetId, (None, None))
				if statusKey != None:
					return (record, index, status)
		return (None, None, None)


class LPTSRecord:

	def __init__(self, record):
		self.record = record

	def recordLen(self):
		return len(self.record.keys())

	## Return the damId's of a record in a map of damId keys
	## result is map damId: [(statusKey, status)]
	def DamIds(self, typeCode, index):
		if not typeCode in {"audio", "text", "video"}:
			print("ERROR: Unknown typeCode '%s', audio, text, or video is expected." % (typeCode))
		if not index in {1, 2, 3}:
			print("ERROR: Unknown DamId index '%s', 1,2, or 3 expected." % (index))
			sys.exit()
		if typeCode == "audio":
			if index == 1:
				damIdDict = {
					"CAudioDAMID1": 		"CAudioStatus1",
					"CAudioDamStockNo": 	"CAudioDamStatus",
					"ND_NTAudioDamID1": 	"ND_NTAudioDamIDStatus1",
					"ND_OTAudioDamID1": 	"ND_OTAudioDamIDStatus1",
					"Reg_NTAudioDamID1": 	"Reg_NTAudioDamIDStatus1",
					"Reg_OTAudioDamID1": 	"Reg_OTAudioDamIDStatus1"
					}
			elif index == 2:
				damIdDict = {
					"ND_CAudioDamID2": 		"ND_CAudioDamIDStatus2",
					"ND_NTAudioDamID2": 	"ND_NTAudioDamIDStatus2",
					"ND_OTAudioDamID2": 	"ND_OTAudioDamIDStatus2",
					"Reg_CAudioDamID2": 	"Reg_CAudioDamIDStatus2",
					"Reg_NTAudioDamID2": 	"Reg_NTAudioDamIDStatus2",
					"Reg_OTAudioDamID2": 	"Reg_OTAudioDamIDStatus2"
					}
			elif index == 3:
				damIdDict = {
					"ND_CAudioDamID3": 		"ND_CAudioDamIDStatus3",
					"ND_NTAudioDamID3": 	"ND_NTAudioDamIDStatus3",
					"ND_OTAudioDamID3": 	"ND_OTAudioDamIDStatus3",
					"Reg_CAudioDamID3": 	"Reg_CAudioDamIDStatus3",
					"Reg_NTAudioDamID3":	"Reg_NTAudioDamIDStatus3",
					"Reg_OTAudioDamID3":	"Reg_OTAudioDamIDStatus3"
					}
		elif typeCode == "text":
			if index == 1:
				damIdDict = {
					"ND_NTTextDamID1": 		"ND_NTTextDamIDStatus1",
					"ND_OTTextDamID1": 		"ND_OTTextDamIDStatus1",
					"Reg_NTTextDamID1": 	"Reg_NTTextDamIDStatus1",
					"Reg_OTTextDamID1": 	"Reg_OTTextDamIDStatus1"
					}
			elif index == 2:
				damIdDict = {
					"ND_NTTextDamID2": 		"ND_NTTextDamIDStatus2",
					"ND_OTTextDamID2": 		"ND_OTTextDamIDStatus2", 
					"Reg_NTTextDamID2": 	"Reg_NTTextDamIDStatus2",
					"Reg_OTTextDamID2": 	"Reg_OTTextDamIDStatus2"
					}
			elif index == 3:
				damIdDict = {
					"ND_NTTextDamID3": 		"ND_NTTextDamIDStatus3",
					"ND_OTTextDamID3": 		"ND_OTTextDamIDStatus3",
					"Reg_NTTextDamID3": 	"Reg_NTTextDamIDStatus3", 
					"Reg_OTTextDamID3": 	"Reg_OTTextDamIDStatus3"
					}
		elif typeCode == "video":
			damIdDict = {
				"Video_John_DamStockNo": "Video_John_DamStatus",
				"Video_Luke_DamStockNo": "Video_Luke_DamStatus",
				"Video_Mark_DamStockNo": "Video_Mark_DamStatus",
				"Video_Matt_DamStockNo": "Video_Matt_DamStatus"
			}	
		hasKeys = set(damIdDict.keys()).intersection(set(self.record.keys()))
		results = {}
		for key in hasKeys:
			statusKey = damIdDict[key]
			damId = self.record[key]
			if typeCode == "text":
				damId = damId[:6]
			status = self.record.get(statusKey)
			results[damId] = (statusKey, status)
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

	def USX_Date(self, index):
		if index == 1:
			return self.record.get("USX_Date1")
		elif index == 2:
			return self.record.get("USX_Date2")
		elif index == 3:
			return self.record.get("USX_Date3")
		else:
			return None

	def PostedToServer(self):
		return self.record.get("PostedToServer")

	def DBPDate(self, index):
		if index == 1:
			return self.record.get("DBPDate")
		elif index == 2:
			return self.record.get("DBPDate2")
		elif index == 3:
			return self.record.get("DBPDate")
		else:
			return None

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


"""
The following fields should be indexed

<HUBLink>https://live.bible.is/bible/PIBWBT/MAT/1?audio_type=audio_drama</HUBLink>
<HUBLink1>https://live.bible.is/bible/PIBWBT/MAT/1?audio_type=audio</HUBLink1>
"""






