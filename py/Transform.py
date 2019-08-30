# Transform.py
# 
# This program handles the transformation of input into data that will be stored in the DBP database.
# It is run after Validation and Clean are run, but before the generation of SQL and database load.
#
# 1. Process the transform for each element of the pkey, and store the result in keyValues
# 2. Once all of the pkey elements are created, we go over them and combine them to create all possible value combinations.  I think this might be a new table specific function.
# 3. We store the key results in the values array for each component of the pkey.
# 4. Now we process all non-key fields, iterating over the existing key fields to create one for each row, if needed.

import hashlib

class Transform:

	def __init__(self, database):
		self.database = database

	def process(self):
		sqlResult = []
		for tbl in self.database.tables:
			if tbl.name in ["bibles", "bible_translations", "bible_filesets", "bible_fileset_connections", 
					"bible_fileset_copyrights",
					"bible_fileset_copyright_organizations", "bible_fileset_tags", "access_group_filesets"]: # Limit processing
				for col in tbl.columns:
					if col.transform != None:
						#print "doCol", tbl.name, col.name, col.transform
						func = getattr(self, col.transform)
						col.values = func(col)
						#print "len col val", len(col.values), col.values
				sqlList = tbl.insertSQL()
				sqlResult += sqlList
				#for sql in sqlList:
				#	print(sql)
		return sqlResult

## Transform Functions

	def assign(self, col):
		return [col.getParamValue(0)]

	def getBibleId(self, col):
		id = col.getParamValue(0)
		if id == None:
			id = col.getParamValue(1)
		return [id]

	def getLanguageId(self, col):
		# The correct procedure will do a lookup in the database
		value = ""
		if col.getParamValue("ISO") != None:
			value += col.getParamValue("ISO")
		if col.getParamValue("langName") != None:
			value += col.getParamValue("langName")
		return [value]

	def getDamIds(self, col):
		damIds = []
		for fld in col.parameters:
			if fld.value is not None:
				damIds.append(fld.value)
		print "damIds", damIds
		return damIds

	def getTypeCode(self, col):
		types = []
		damIds = col.getVariable("id")
		for damId in damIds:
			if damId[-3:] == "1DA":
				types.append("audio")
			elif damId[-3:] == "2DA":
				types.append("audio_drama")
			else:
				types.append("text_format")
			## not finished, also text_plain, video_stream
		return types

	def getHashId(self, col):
		ids = []
		damIds = col.getVariable("id")
		types = col.getVariable("set_type_code")
		bucket = "dbp-prod" ### This needs to be read in from a config file
		md5 = hashlib.md5()
		for index in range(len(damIds)):
			damId = damIds[index]
			fsType = types[index]
			md5.update(damId + bucket + fsType)
			hash_id = md5.hexdigest()
			ids.append(hash_id[:12])
		return ids

	def getAssetId(self, col):
		assetIds = []
		types = col.getVariable("set_type_code")
		bucket = "dbp-prod" ### Needs to come from config file
		vid_bucket = "vb-prod" ## from config file
		for typ in types:
			if typ.count("video") > 0:
				assetIds.append(vid_bucket)
			else:
				assetIds.append(bucket)
		return assetIds

	def copyValues(self, col):
		return col.getVariable(0)

	def getCopyright(self, col):
		copyc = col.getParamValue("Copyrightc")
		copyp = col.getParamValue("Copyrightp")
		copyv = col.getParamValue("Copyright_Video")

		if copyc != None and copyp != None:
			audioCopyright = "Text: %s\nAudio: %s" % (copyc, copyp)
		elif copyp != None:
			audioCopyright = "Audio: %s" % (copyp)
		else:
			audioCopyright = None
		
		if copyc != None:
			textCopyright = copyc
		elif copyp != None:
			textCopyright = copyp
		else:
			textCopyright = None
		
		if copyv != None and copyc != None and copyp != None:
			videoCopyright = "Text: %s\nAudio: %s\nVideo: %s" % (copyc, copyp, copyv)
		elif copyv != None:
			videoCopyright = "Video: %s" % (copyv)
		else:
			videoCopyright = None

		typeValues = col.getVariable("set_type_code")
		copyList = []
		for typ in typeValues:
			if typ.count("audio") > 0:
				copyList.append(audioCopyright)
			elif typ.count("text") > 0:
				copyList.append(textCopyright)
			elif typ.count("video") > 0:
				copyList.append(videoCopyright)
		return copyList

	def getOrganizationId(self, col):
		orgLicensor = col.getParamValue("copyrightOrganizationLicensor")
		audioCopyright = col.getParamValue("Copyrightp")
		if audioCopyright is not None and audioCopyright.count("Hosanna")>0:
			audioLicensor = "9"
		elif audioCopyright is not None and audioCopyright.count("Mitla Studio")>0:
			audioLicensor = "238"
		elif audioCopyright is not None and audioCopyright.count("Wycliffe Inc")>0:
			audioLicensor = "30"
		else:
			audioLicensor = None

		typeValues = col.getVariable("set_type_code")
		orgIdList = []
		for typ in typeValues:
			if typ.count("audio") > 0:
				orgIdList.append(audioLicensor)
			else:
				orgIdList.append(orgLicensor)
		return orgIdList

	def getFilesetTag(self, col):
		hashIds = col.getVariable("hash_id")
		damIds = col.getVariable("id")
		sku = col.getParamValue("Reg_StockNumber")
		vol = col.getParamValue("Volumne_Name")

		resultHashIds = []
		resultTagNames = []
		resultTagDesc = []

		for index in range(len(hashIds)):
			hashId = hashIds[index]
			if sku != None:
				resultHashIds.append(hashId)
				resultTagNames.append("sku")
				resultTagDesc.append(sku)
			if vol != None:
				resultHashIds.append(hashId)
				resultTagNames.append("volume")
				resultTagDesc.append(vol)
			damId = damIds[index]
			if damId[8:10] == "DA": # Is this the correct test for Audio?
				if damId[-2:] == "16":
					bitrate = "16kbps"
				else:
					bitrate = "64kbps"
				resultHashIds.append(hashId)
				resultTagNames.append("bitrate")
				resultTagDesc.append(bitrate)

		col.table.columnMap["hash_id"].values = resultHashIds # output to hash_id column
		col.table.columnMap["name"].values = resultTagNames # ouput to name column
		return resultTagDesc

	def getAccessGroups(self, col):
		groupNames = { 	"DBPText": "",
						"DBPTextOT": "",
						"DBPAudio": "",
						"DBPMobile": "",
						"DBPWebHub": "",
						"GBN_Text": "",
						"GBN_Audio": "",
						"Download": "",
						"Streaming": "",
						"FCBHStore": "",
						"ItunesPodcast": "",
						"APIDevText": "",
						"APIDevAudio": "",
						"MobileText": "",
						"HubText": "",
						"MobileVideo": "",
						"WebHubVideo": "",
						"APIDevVideo": "",
						"GBN_Video": "",
						"DownloadVideo": "",
						"StreamingVideo": "",
						"FCBHStoreVideo": "",
						"ItunesPodcastVideo": "" }
		#accessGranted = col.getParamValue("access_granted")
		#accessRestricted = col.getParamValue("access_restricted")
		#accessVideo = col.getParamValue("access_video")
		resultHashIds = []
		resultAccess = []
		hashIds = col.getVariable("hash_id")
		for hashId in hashIds:
			for param in col.parameters:
				if param.type == "lptsXML":
					#print param.type, param.name, param.value
					if param.value == "-1":
						resultHashIds.append(hashId)
						resultAccess.append(param.name)

		col.table.columnMap["hash_id"].values = resultHashIds
		return resultAccess


	def tbd(self, col):
		value = ""
		for parm in col.parameters:
			value += parm.name
		return [value]


