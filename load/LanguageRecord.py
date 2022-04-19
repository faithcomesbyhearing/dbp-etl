from LanguageReader import LanguageRecordInterface

class LanguageRecord (LanguageRecordInterface):

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
					list(languageRecord.audio3DamIdDict.items()) 
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
			damIdOut = damId[:7] + "_" + damId[8:]
			damIdSet.add((damIdOut, index, status))
		return damIdSet


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
