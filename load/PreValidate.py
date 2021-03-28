# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.

# This class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LPTSExtractReader import *

class PreValidate:


	def __init__(self, lptsReader):
		self.lptsReader = lptsReader
		self.filesetIds = []
		self.messages = {}


	def validateFilesetId(self, filesetId):
		results = self.lptsReader.getFilesetRecords10(filesetId) # This method expect 10 digit DamId's always
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
			if len(mediaSet) > 1:
				self.errorMessage(filesetId, "in %s has more than one media type: %s" % (", ".join(stockNumSet), ", ".join(mediaSet)))
			if len(bibleIdSet) == 0:
				self.errorMessage(filesetId, "in %s does not have a DBP_Equivalent" % (", ".join(stockNumSet)))
			if len(bibleIdSet) > 1:
				self.errorMessage(filesetId, "in %s has more than one DBP_Equivalent: %s" % (", ".join(stockNumSet), ", ".join(bibleIdSet)))

			if len(mediaSet) > 0 and len(bibleIdSet) > 0:
				return (list(mediaSet)[0], list(bibleIdSet)[0])
			else:
				return None


	def validateLPTS(self, typeCode, bibleId, filesetId):
		(lptsRecord, index) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
		if lptsRecord == None:
			self.errorMessage(filesetId, "filesetId is not in LPTS record.")
			return None
		else:
			stockNo = lptsRecord.Reg_StockNumber()
			bibleIdFound = lptsRecord.DBP_EquivalentByIndex(index)
			if bibleIdFound == None:
				self.requiredFields(filesetId, stockNo, "DBP_Equivalent (BibleId)")
			elif bibleIdFound != bibleId:
				self.errorMessage(filesetId, "BibleId given %s is not BibleId in LPTS %s" % (bibleId, bibleIdFound))
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
	lptsReader = LPTSExtractReader(os.environ["LPTS_XML"])
	validate = PreValidate(lptsReader)
	data = None
	try:
		data = json.load(sys.stdin)
	except Exception as err:
		errors = []
		errors.append("Error parsing JSON input")
		validate.messages["UNKNOWN"] = errors
		print(json.dumps(validate.messages))
		sys.exit()
	for item in data:
		filesetId = item.get("prefix")
		validate.filesetIds.append(filesetId)
		prefix = validate.validateFilesetId(filesetId)
		if prefix != None:
			(typeCode, bibleId) = prefix
			if typeCode == "text":
				filesetId = filesetId[:6]
			validate.validateLPTS(typeCode, bibleId, filesetId)
	validate.printLog()

