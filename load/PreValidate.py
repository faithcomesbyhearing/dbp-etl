# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LPTSExtractReader import *

class PreValidate:


	def __init__(self, lptsReader):
		self.lptsReader = lptsReader
		self.filesetIds = []
		self.messages = {}
		self.OT = { "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", 
					"EZR", "NEH", "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", 
					"JOL", "AMO", "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL" }
		self.NT = { "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH", "PHP", "COL", "1TH", "2TH", 
					"1TI", "2TI", "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV" }


    ## Validate input and return (lptsRecord, filesetId, damId, typeCode, bibleId, index)
	def validate(self, directory, filenames):
		parts = directory.split("_")
		if len(parts) > 2:
			stockNo = parts[-2]
			if len(stockNo) > 5:
				stockNumber = stockNo[:-3] + "/" + stockNo[-3:]
				return self.validateUSXStockNo(stockNumber, filenames)
			else:
				self.errorMessage(directory, "Could not find a stock no.")
		else:
			return self.validateFilesetId(directory)


    ## Validate filesetId and return (lptsRecord, damId, typeCode, bibleId, index)
	def validateFilesetId(self, filesetId):
		self.filesetIds.append(filesetId)
		results = self.lptsReader.getFilesetRecords10(filesetId) # This method expects 10 digit DamId's always
		if results == None:
			self.errorMessage(filesetId, "is not in LPTS")
			return None
		else:
			stockNumSet = set()
			mediaSet = set()
			bibleIdSet = set()
			for (lptsRecord, status, fieldName) in results:
				stockNum = lptsRecord.Reg_StockNumber()
				stockNumSet.add(stockNum)
				damId = lptsRecord.record.get(fieldName)

				dbpFilesetId = filesetId
				if "Audio" in fieldName:
					media = "audio"
				elif "Text" in fieldName:
					media = "text"
					dbpFilesetId = filesetId[:6]
				elif "Video" in fieldName:
					media = "video"
				else:
					media = "unknown"
					self.errorMessage(filesetId, "in %s does not have Audio, Text, or Video in DamId fieldname." % (stockNum,))
				mediaSet.add(media)

				if "3" in fieldName:
					index = 3
				elif "2" in fieldName:
					index = 2
				else:
					index = 1

				bibleId = lptsRecord.DBP_EquivalentByIndex(index)
				bibleIdSet.add(bibleId)
			if len(stockNumSet) > 1:
				self.errorMessage(filesetId, "has more than one stock no: %s" % (", ".join(stockNumSet)))
			if len(mediaSet) > 1:
				self.errorMessage(filesetId, "in %s has more than one media type: %s" % (", ".join(stockNumSet), ", ".join(mediaSet)))
			if len(bibleIdSet) == 0:
				self.errorMessage(filesetId, "in %s does not have a DBP_Equivalent" % (", ".join(stockNumSet)))
			if len(bibleIdSet) > 1:
				self.errorMessage(filesetId, "in %s has more than one DBP_Equivalent: %s" % (", ".join(stockNumSet), ", ".join(bibleIdSet)))

			if len(mediaSet) > 0 and len(bibleIdSet) > 0:
				return (lptsRecord, damId, list(mediaSet)[0], list(bibleIdSet)[0], index)
			else:
				return None


	## Validate filesetId and return (lptsRecord, damId, typeCode, bibleId, index)
	def validateUSXStockNo(self, stockNo, filenames):
		self.filesetIds.append(stockNo)
		typeCode = "text"
		lptsRecord = self.lptsReader.getByStockNumber(stockNo)
		if lptsRecord == None:
			self.errorMessage(stockNo, "is not in LPTS")
			return None
		else:
			results = []
			expectedScope = self._getExpectedScope(filenames)
			print("expect scope", expectedScope)
			textDamIds = lptsRecord.DamIdList(typeCode)
			textDamIds = lptsRecord.ReduceTextList(textDamIds)
			print("damIds", textDamIds)
			for (damId, index, status) in textDamIds:
				print("looking for", damId[6])
				if damId[6] == expectedScope:
					results.append((damId, index, status))

			if len(results) == 0:
				self.errorMessage(stockNo, "Has no filesets of scope %s" % (expectedScope,))
				return None
			(damId0, index0, status0) = results[0]
			bibleId = lptsRecord.DBP_EquivalentByIndex(index0)
			return (lptsRecord, damId0, typeCode, bibleId, index0) # There can be multiple


	def _getExpectedScope(self, filenames):
		bookIdSet = set()
		for filename in filenames:
			name = filename.split(".")[0]
			bookId = name[-3:]
			bookIdSet.add(bookId)
		otBooks = bookIdSet.intersection(self.OT)
		ntBooks = bookIdSet.intersection(self.NT)
		if len(otBooks) >= 39 and len(ntBooks) > 27:
			return "C"
		elif len(ntBooks) >= 27:
			return "N"
		elif len(otBooks) >= 39:
			return "O"
		else:
			return "P"


	def validateLPTS(self, typeCode, filesetId, lptsRecord, index):
		stockNo = lptsRecord.Reg_StockNumber()
		if lptsRecord.Copyrightc() == None:
			self.requiredFields(filesetId, stockNo, "Copyrightc")
		if typeCode in {"audio", "video"} and lptsRecord.Copyrightp() == None:
			self.requiredFields(filesetId, stockNo, "Copyrightp")
		if typeCode == "video" and lptsRecord.Copyright_Video() == None:
			self.requiredFields(filesetId, stockNo, "Copyright_Video")
		if lptsRecord.ISO() == None:
			self.requiredFields(filesetId, stockNo, "ISO")
		if lptsRecord.LangName() == None:
			self.requiredFields(filesetId, stockNo, "LangName")
		if lptsRecord.Licensor() == None:
			self.requiredFields(filesetId, stockNo, "Licensor")
		if lptsRecord.Reg_StockNumber() == None:
			self.requiredFields(filesetId, stockNo, "Reg_StockNumber")
		if lptsRecord.Volumne_Name() == None:
			self.requiredFields(filesetId, stockNo, "Volumne_Name")

		if typeCode == "text" and lptsRecord.Orthography(index) == None:
			fieldName = "_x003%d_Orthography" % (index)
			self.requiredFields(filesetId, stockNo, fieldName)


	def errorMessage(self, filesetId, message):
		errors = self.messages.get(filesetId, [])
		errors.append(message)
		self.messages[filesetId] = errors


	def requiredFields(self, filesetId, stockNo, fieldName):
		message = "LPTS %s field %s is required." % (stockNo, fieldName)
		self.errorMessage(filesetId, message)


	def printLog(self):
		for filesetId in self.filesetIds:
			if self.hasErrors(filesetId):
				fullErrors = []
				errors = self.messages.get(filesetId)
				if errors != None:
					for error in errors:
						fullErrors.append("ERROR %s %s" % (filesetId, error))
					self.messages[filesetId] = fullErrors
			else:
				errors = []
				errors.append("INFO %s PreValidation OK" % (filesetId,))
				self.messages[filesetId] = errors
		result = json.dumps(self.messages)
		print(result)	


	def hasErrors(self, filesetId):
		has = self.messages.get(filesetId)
		if has == None and filesetId.endswith("ET"):
			has = self.messages.get(filesetId[:6])
		return has != None


if (__name__ == '__main__'):
	oldT = { "prefix": "$", "files": [ "GEN.usx", "EXO.usx", "LEV.usx", "NUM.usx", "DEU.usx", "JOS.usx", "JDG.usx", 
										"RUT.usx", "1SA.usx", "2SA.usx", "1KI.usx", "2KI.usx", "1CH.usx", "2CH.usx", 
										"EZR.usx", "NEH.usx", "EST.usx", "JOB.usx", "PSA.usx", "PRO.usx", "ECC.usx", 
										"SNG.usx", "ISA.usx", "JER.usx", "LAM.usx", "EZK.usx", "DAN.usx", "HOS.usx", 
										"JOL.usx", "AMO.usx", "OBA.usx", "JON.usx", "MIC.usx", "NAM.usx", "HAB.usx", 
										"ZEP.usx", "HAG.usx", "ZEC.usx", "MAL.usx" ] }
	newT = { "prefix": "$", "files": [ "040MAT.usx", "041MRK.usx", "042LUK.usx", "043JHN.usx", "044ACT.usx", 
									"045ROM.usx", "0461CO.usx", "0472CO.usx", "048GAL.usx", "049EPH.usx", 
									"050PHP.usx", "051COL.usx", "0521TH.usx", "0532TH.usx", "0541TI.usx", 
									"0552TI.usx", "056TIT.usx", "057PHM.usx", "058HEB.usx", "059JAS.usx", 
									"0601PE.usx", "0612PE.usx", "0621JN.usx", "0632JN.usx", "0643JN.usx", 
									"065JUD.usx", "066REV.usx"] }
	partT = { "prefix": "$", "files": [ "040MAT.usx", "041MRK.usx", "042LUK.usx", "043JHN.usx" ] }
	lptsReader = LPTSExtractReader(os.environ["LPTS_XML"])
	for rec in lptsReader.resultSet:
		directory = rec.LangName() + "_" + rec.Reg_StockNumber().replace("/", "") + "_USX"
		for message in [ oldT, newT, partT ]:
			message["prefix"] = directory
			preValidate = PreValidate(lptsReader)
			directory = message.get("prefix")
			filenames = message.get("files")
			lptsData = preValidate.validate(directory, filenames)
			if lptsData != None:
				(lptsRecord, damId, typeCode, bibleId, index) = lptsData
				print("found", damId, bibleId, index)
				preValidate.validateLPTS(typeCode, damId, lptsRecord, index)
			preValidate.printLog()
			print("*********")




	

