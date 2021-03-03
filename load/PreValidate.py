# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.

# This class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LPTSExtractReader import *

class PreValidate:


	def __init__(self, lptsReader):
		self.lptsReader = lptsReader
		self.messages = []


	def validateFilesetId(self, filesetId):
		print(filesetId)
		results = self.lptsReader.getFilesetRecords10(filesetId) # This method expect 10 digit DamId's always
		if results == None:
			self.errorMessage("is not in LPTS")
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
					self.errorMessage("in %s does not have Audio, Text, or Video in DamId fieldname." % (stockNum,))
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
				self.errorMessage("in %s has more than one media type: %s" % (", ".join(stockNumSet), ", ".join(mediaSet)))
			if len(bibleIdSet) == 0:
				self.errorMessage("in %s does not have a DBP_Equivalent" % (", ".join(stockNumSet)))
			if len(bibleIdSet) > 1:
				self.errorMessage("in %s has more than one DBP_Equivalent: %s" % (", ".join(stockNumSet), ", ".join(bibleIdSet)))

			if len(mediaSet) > 0 and len(bibleIdSet) > 0:
				return (list(mediaSet)[0], list(bibleIdSet)[0])
			else:
				return None


	def validateLPTS(self, typeCode, bibleId, filesetId):
		(lptsRecord, index) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
		if lptsRecord == None:
			self.errorMessage("filesetId is not in LPTS record.")
			return None
		else:
			stockNo = lptsRecord.Reg_StockNumber()
			bibleIdFound = lptsRecord.DBP_EquivalentByIndex(index)
			if bibleIdFound == None:
				self.requiredFields(stockNo, "DBP_Equivalent (BibleId)")
			elif bibleIdFound != bibleId:
				self.errorMessage(stockNo, "BibleId given %s is not BibleId in LPTS %s" % (bibleId, bibleIdFound))
			if lptsRecord.Copyrightc() == None:
				self.requiredFields(stockNo, "Copyrightc")
			if typeCode in {"audio", "video"} and lptsRecord.Copyrightp() == None:
				self.requiredFields(stockNo, "Copyrightp")
			if typeCode == "video" and lptsRecord.Copyright_Video() == None:
				self.requiredFields(stockNo, "Copyright_Video")
			if lptsRecord.ISO() == None:
				self.requiredFields(stockNo, "ISO")
			if lptsRecord.LangName() == None:
				self.requiredFields(stockNo, "LangName")
			if lptsRecord.Licensor() == None:
				self.requiredFields(stockNo, "Licensor")
			if lptsRecord.Reg_StockNumber() == None:
				self.requiredFields(stockNo, "Reg_StockNumber")
			if lptsRecord.Volumne_Name() == None:
				self.requiredFields(stockNo, "Volumne_Name")

			if typeCode == "text" and lptsRecord.Orthography(index) == None:
				fieldName = "_x003%d_Orthography" % (index)
				self.requiredFields(stockNo, fieldName)


	def errorMessage(self, message):
		self.messages.append(message)


	def requiredFields(self, stockNo, fieldName):
		self.messages.append("LPTS %s field %s is required." % (stockNo, fieldName))


	def printLog(self, filesetId):
		if len(self.messages) > 0:
			for message in self.messages:
				print("ERROR %s %s" % (filesetId, message))
		else:
			print("INFO %s PreValidation OK" % (filesetId,))


if (__name__ == '__main__'):
	lptsReader = LPTSExtractReader(os.environ["LPTS_XML"])
	validate = PreValidate(lptsReader)
	data = json.load(sys.stdin)
	filesetId = data.get("prefix")
	prefix = validate.validateFilesetId(filesetId)
	if prefix != None and len(validate.messages) == 0:
		(typeCode, bibleId) = prefix
		#print(typeCode, bibleId)
		if typeCode == "text":
			filesetId = filesetId[:6]
		validate.validateLPTS(typeCode, bibleId, filesetId)
	validate.printLog(filesetId)

