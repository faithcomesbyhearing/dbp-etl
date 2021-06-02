# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LPTSExtractReader import *
from UnicodeScript import *


class PreValidateResult:

	def __init__(self, lptsRecord, filesetId, damId, typeCode, index):
		self.lptsRecord = lptsRecord
		self.filesetId = filesetId
		self.damId = damId
		self.typeCode = typeCode
		self.bibleId = lptsRecord.DBP_EquivalentByIndex(index)
		self.index = index

	def toString(self):
		return("out: %s/%s/%s is %s %d" % (self.typeCode, self.bibleId, self.filesetId, self.damId, self.index))		


class PreValidate:


	## Validate input and return an errorList.
	def validateLambda(lptsReader, directory, filenames):
		unicodeScript = UnicodeScript()
		preValidate = PreValidate(lptsReader, unicodeScript)
		result = None
		stockNumber = preValidate.isTextStockNo(directory)
		if stockNumber != None:
			resultList = preValidate.validateUSXStockNo(stockNumber, filenames)
			if resultList != None and len(resultList) > 0:
				result = resultList[0]
		else:
			result = preValidate.validateFilesetId(directory)
		if result != None:
			preValidate.validateLPTS(result)
		preValidate.addErrorMessages(directory, unicodeScript.errors)
		return preValidate.formatMessages()


	def validateDBPELT(lptsReader, s3Client, location, directory, fullPath):
		unicodeScript = UnicodeScript()
		preValidate = PreValidate(lptsReader, unicodeScript)
		result = None
		stockNumber = preValidate.isTextStockNo(directory)
		if stockNumber != None:
			filenames = unicodeScript.getFilenames(s3Client, location, fullPath)
			resultList = preValidate.validateUSXStockNo(stockNumber, filenames)
			if resultList != None:
				sampleText = unicodeScript.readObject(s3Client, location, filenames[0])
				result = preValidate.validateScript(stockNumber, resultList, sampleText)
		else:
			result = preValidate.validateFilesetId(directory)
		if result != None:
			preValidate.validateLPTS(result)
		preValidate.addErrorMessages(directory, unicodeScript.errors)
		return (result, preValidate.messages)


	def __init__(self, lptsReader, unicodeScript):
		self.lptsReader = lptsReader
		self.unicodeScript = unicodeScript
		self.messages = {}
		self.OT = { "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", 
					"EZR", "NEH", "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", 
					"JOL", "AMO", "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL" }
		self.NT = { "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH", "PHP", "COL", "1TH", "2TH", 
					"1TI", "2TI", "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV" }


	## determine if directory is USX text stockNo
	def isTextStockNo(self, directory):
		parts = directory.split("_")
		if len(parts) > 2:
			stockNo = parts[-2]
			if len(stockNo) > 5:
				stockNumber = stockNo[:-3] + "/" + stockNo[-3:]
				return stockNumber
			else:
				self.errorMessage(directory, "Could not find a stock no.")
		return None


    ## Validate filesetId and return PreValidateResult
	def validateFilesetId(self, filesetId):
		#print("validate filesetId", filesetId)
		damId = filesetId.split("-")[0]
		damId = damId.replace("_", "2")
		results = self.lptsReader.getFilesetRecords10(damId) # This method expects 10 digit DamId's always
		if results == None:
			self.errorMessage(damId, "filesetId is not in LPTS")
			return None
		else:
			stockNumSet = set()
			mediaSet = set()
			bibleIdSet = set()
			for (lptsRecord, status, fieldName) in results:
				stockNum = lptsRecord.Reg_StockNumber()
				if stockNum != None:
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
				if bibleId != None:
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
				return PreValidateResult(lptsRecord, filesetId, damId, list(mediaSet)[0], index)
			else:
				return None


	## Validate filesetId and return PreValidateResult
	def validateUSXStockNo(self, stockNo, filenames):
		lptsRecord = self.lptsReader.getByStockNumber(stockNo)
		if lptsRecord == None:
			self.errorMessage(stockNo, "stockNum is not in LPTS")
			return None
		else:
			results = []
			actualScope = self._getExpectedScope(filenames)
			#print("expect scope", actualScope)
			textDamIds = lptsRecord.DamIdList("text")
			textDamIds = lptsRecord.ReduceTextList(textDamIds)
			#print("damIds", textDamIds)
			for (damId, index, status) in textDamIds:
				#print("looking for", damId[6])
				if damId[6] == actualScope:
					filesetId = damId[:7] + "_" + damId[8:] + "-usx"
					result = PreValidateResult(lptsRecord, filesetId, damId, "text", index)
					results.append(result)
			if len(results) == 0:
				self.errorMessage(stockNo, "Has no filesets with scope %s" % (actualScope,))
				return None
			else:
				return results


	## Validate that the script code is correct and return matching PreValidateResult
	def validateScript(self, stockNo, preValidateResults, sampleText):
		textArray = self.unicodeScript.parseXMLStrings(sampleText)
		#print("sample", "".join(textArray[:40]))
		(actualScript, matchPct) = self.unicodeScript.findScript(textArray)	
		#print("actualScript", actualScript)
		for result in preValidateResults:
			lptsScript = result.lptsRecord.Orthography(result.index)
			#print("lptsScript", lptsScript, result.index)
			if self.unicodeScript.matchScripts(actualScript, lptsScript):
				return result
		self.errorMessage(stockNo, "Has no filesets with correct scope and script %s" % (actualScript,))
		return None


	def _getExpectedScope(self, filenames):
		#print("filenames", filenames)
		bookIdSet = set()
		for filename in filenames:
			name = filename.split(".")[0]
			bookId = name[-3:]
			bookIdSet.add(bookId)
		#print("books", bookIdSet)
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


	def validateLPTS(self, preValidateResult):
		pre = preValidateResult
		stockNo = pre.lptsRecord.Reg_StockNumber()
		if pre.lptsRecord.Copyrightc() == None:
			self.requiredFields(pre.filesetId, stockNo, "Copyrightc")
		if pre.typeCode in {"audio", "video"} and pre.lptsRecord.Copyrightp() == None:
			self.requiredFields(pre.filesetId, stockNo, "Copyrightp")
		if pre.typeCode == "video" and pre.lptsRecord.Copyright_Video() == None:
			self.requiredFields(pre.filesetId, stockNo, "Copyright_Video")
		if pre.lptsRecord.ISO() == None:
			self.requiredFields(pre.filesetId, stockNo, "ISO")
		if pre.lptsRecord.LangName() == None:
			self.requiredFields(pre.filesetId, stockNo, "LangName")
		if pre.lptsRecord.Licensor() == None:
			self.requiredFields(pre.filesetId, stockNo, "Licensor")
		if pre.lptsRecord.Reg_StockNumber() == None:
			self.requiredFields(pre.filesetId, stockNo, "Reg_StockNumber")
		if pre.lptsRecord.Volumne_Name() == None:
			self.requiredFields(pre.filesetId, stockNo, "Volumne_Name")

		if pre.typeCode == "text" and pre.lptsRecord.Orthography(pre.index) == None:
			fieldName = "_x003%d_Orthography" % (pre.index)
			self.requiredFields(pre.filesetId, stockNo, fieldName)


	def addErrorMessages(self, identifier, messages):
		if messages != None and len(messages) > 0:
			errors = self.messages.get(identifier, [])
			for message in messages:
				errors.append(message)
			self.messages[identifier] = errors


	def errorMessage(self, identifier, message):
		errors = self.messages.get(identifier, [])
		errors.append(message)
		self.messages[identifier] = errors


	def requiredFields(self, filesetId, stockNo, fieldName):
		message = "LPTS %s field %s is required." % (stockNo, fieldName)
		self.errorMessage(filesetId, message)


	def formatMessages(self):
		results = []
		if len(self.messages.keys()) > 0:
			for (identifier, errors) in self.messages.items():
				for error in errors:
					results.append("ERROR %s %s" % (identifier, error))
		#else:
		#	results.append("INFO PreValidate OK")
		return results


	def hasErrors(self):
		return len(self.messages) > 0


## System Test of PreValidate for DBP-ETL
if (__name__ == "__main__"):
	from AWSSession import *
	from Config import *
	from SQLUtility import *
	from InputFileset import *	

	config = Config.shared()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(os.environ["LPTS_XML"])
	s3Client = AWSSession.shared().s3Client
	location = "s3://dbp-prod"
	#location = "s3://dbp-staging"
	sql = ("SELECT c.bible_id, f.id, f.set_type_code"
		" FROM bible_fileset_connections c, bible_filesets f"
		" WHERE f.hash_id = c.hash_id"
		" AND set_type_code IN ('text_usx', 'audio_drama2')"
		" AND set_size_code != 'S'"
		" ORDER BY c.bible_id, f.id")
	resultSet = db.select(sql, ())
	for (bibleId, filesetId, setTypeCode) in resultSet:
		typeCode = setTypeCode.split("_")[0]
		filesetPath = "%s/%s/%s" % (typeCode, bibleId, filesetId)
		print("input:", filesetPath)

		## Do validateDBPELT Test
		if typeCode == "audio":
			(inp, messages) = PreValidate.validateDBPELT(lptsReader, s3Client, location, filesetPath)
		elif typeCode == "text":
			(lptsRecord, index) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
			stockNo = lptsRecord.Reg_StockNumber().replace("/", "")
			directory = "/ABCD_" + stockNo + "_USX"
			#filesetPath += directory
			(inp, messages) = PreValidate.validateDBPELT(lptsReader, s3Client, location, directory, filesetPath)
		if messages != None and len(messages) > 0:
			Log.addPreValidationErrors(messages)
			print("DBP-ELT", messages)
		if inp != None:
			result = InputFileset(config, location, inp.filesetId, filesetPath, inp.damId, inp.typeCode, inp.bibleId, inp.index, inp.lptsRecord)
			#print("dbp-out", result.toString())

		## Do validateLambda Test
		directory = None
		filenames = []
		if typeCode == "audio":
			filenames = []
			directory = filesetId
		elif typeCode == "text":
			stockNo = lptsRecord.Reg_StockNumber().replace("/", "")
			directory = "ABCD_" + stockNo + "_USX"
			unicodeScript = UnicodeScript()
			filenames = unicodeScript.getFilenames(s3Client, location, filesetPath)
		print(filesetPath, filenames)
		lambdaMessages = PreValidate.validateLambda(lptsReader, directory, filenames)
		if lambdaMessages != None and len(lambdaMessages) > 0:
			print("Lambda", lambdaMessages)



# time python3 load/PreValidate.py newdata
	

