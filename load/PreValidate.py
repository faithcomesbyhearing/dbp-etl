# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LPTSExtractReader import *
from UnicodeScript import *


class PreValidateResult:

	def __init__(self, lptsRecord, filesetId, damId, typeCode, index, fileSet = None):
		self.lptsRecord = lptsRecord
		self.filesetId = filesetId
		self.damId = damId
		self.typeCode = typeCode
		self.bibleId = lptsRecord.DBP_EquivalentByIndex(index)
		self.index = index
		self.fileSet = fileSet

	def scope(self):
		return self.damId[6]

	def script(self):
		return self.lptsRecord.Orthography(self.index)

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
		resultList = []
		stockNumber = preValidate.isTextStockNo(directory)
		if stockNumber != None:
			filenames = unicodeScript.getFilenames(s3Client, location, fullPath)
			sampleText = unicodeScript.readObject(s3Client, location, filenames[0])
			resultList = preValidate.validateUSXStockNo(stockNumber, filenames, sampleText)
		else:
			result = preValidate.validateFilesetId(directory)
			if result != None:
				resultList = [result]
		if len(resultList) > 0:
			preValidate.validateLPTS(resultList[0])
		preValidate.addErrorMessages(directory, unicodeScript.errors)
		return (resultList, preValidate.messages)


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
		filesetId1 = filesetId.split("-")[0]
		damId = filesetId1.replace("_", "2")
		results = self.lptsReader.getFilesetRecords10(damId) # This method expects 10 digit DamId's always
		if results == None:
			damId = filesetId1.replace("_", "1")
			results = self.lptsReader.getFilesetRecords10(damId)
			if results == None:
				self.errorMessage(filesetId1, "filesetId is not in LPTS")
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


	def validateUSXStockNo( self, stockNo, filenames, sampleText = None):
		lptsRecord = self.lptsReader.getByStockNumber(stockNo)
		if lptsRecord == None:
			self.errorMessage(stockNo, "stockNum is not in LPTS")
			return []
		scopeMap = self._findDamIdScopeMap(stockNo, lptsRecord) # scopeMap is { scope: [(damId, script)] }
		bookIdMap = self._findBookIdMap(stockNo, filenames) # bookIdMap is { bookId: filename }
		if sampleText != None:
			textArray = self.unicodeScript.parseXMLStrings(sampleText)
			#print("sample", "".join(textArray[:40]))
			(actualScript, matchPct) = self.unicodeScript.findScript(textArray)
		else:
			actualScript = None
		ntResult = self._matchFilesToDamId(stockNo, "N", lptsRecord, scopeMap, bookIdMap, actualScript) # PreValidateResult or None
		#if ntResult != None:
		#	print("NT", ntResult.toString())
		otResult = self._matchFilesToDamId(stockNo, "O", lptsRecord, scopeMap, bookIdMap, actualScript) # PreValidateResult or None
		#if otResult != None:
		#	print("OT", otResult.toString())
		bothResults = self._combineMultiplePScope(stockNo, ntResult, otResult)
		#for result in bothResults:
		#	print("BOTH", result.toString())
		return bothResults


	## This method returns a map { scope: [(damId, script)] } from LPTS for a stock number
	def _findDamIdScopeMap(self, stockNo, lptsRecord):
		result = {}
		textDamIds = lptsRecord.DamIdList("text")
		textDamIds = lptsRecord.ReduceTextList(textDamIds)
		#print("damIds", textDamIds)
		for (damId, index, status) in textDamIds:
			scope = damId[6]
			script = lptsRecord.Orthography(index)
			damIdList = result.get(scope, [])
			damIdList.append((damId, index, script))
			result[scope] = damIdList
		return result


	## This method returns a map { bookId: filename } from input filenames
	def _findBookIdMap(self, stockNo, filenames):
		result = {}
		for filename in filenames:
			name = filename.split(".")[0]
			bookId = name[-3:]
			if result.get(bookId) != None:
				self.errorMessage(stockNo, "Has duplicate book_id %s" % (bookId,))
			result[bookId] = filename
		return result


	## Match files included in 
	def _matchFilesToDamId(self, stockNo, scope, lptsRecord, scopeMap, bookIdMap, actualScript = None):
		result = None
		if scope == "N":
			bookIdSet = self.NT 
		else:
			bookIdSet = self.OT 
		bookIdsFound = bookIdSet.intersection(bookIdMap.keys())
		if len(bookIdsFound) >= len(bookIdSet):
			if scopeMap.get(scope) != None:
				for (damId, index, script) in scopeMap.get(scope):
					if actualScript == None or self.unicodeScript.matchScripts(actualScript, script):
						filenameSet = self._getFilenameSet(scope, bookIdsFound, bookIdMap)
						result = PreValidateResult(lptsRecord, damId + "-usx", damId, "text", index, filenameSet)
				if result == None:
					self.errorMessage(stockNo, "text is script %s, but there is no damId with that script in scope %s." % (actualScript, scope))
			else:
				self.errorMessage(stockNo, "contains a full set of books for %s, but there is no damId with that scope" % (scope,))
		elif len(bookIdsFound) > 0:
			if scopeMap.get("P") != None:
				for (damId, index, script) in scopeMap.get("P"):
					if actualScript == None or self.unicodeScript.matchScripts(actualScript, script):
						filenameSet = self._getFilenameSet(scope, bookIdsFound, bookIdMap)
						result = PreValidateResult(lptsRecord, damId + "-usx", damId, "text", index, filenameSet)
				if result == None:
					self.errorMessage(stockNo, "text is script %s, but there is no damId with that script in scope P." % (actualScript,))
			else:
				self.errorMessage(stockNo, "contains some books, but there is no damId with scope P")
		return result


	def _getFilenameSet(self, nToT, selectBookIds, bookIdMap):
		result = set()
		if nToT == "N":
			includeBooks = selectBookIds
		else:
			includeBooks = set(bookIdMap.keys()).difference(self.NT)
		for bookId in includeBooks:
			filename = bookIdMap[bookId]
			result.add(filename)
		return result


	def _combineMultiplePScope(self, stockNo, ntResult, otResult):
		results = []
		if ntResult != None and otResult != None:
			if ntResult.scope() == "P" and otResult.scope() == "P":
				fullFiles = ntResult.fileSet.union(otResult.fileSet)
				ntResult.fileSet = fullFiles
				results.append(ntResult)
		if ntResult != None:
			results.append(ntResult)
		if otResult != None:
			results.append(otResult)
		return results


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
		return results


	def hasErrors(self):
		return len(self.messages) > 0


## Test data
## aws s3 ls --profile DBP_DEV s3://dbp-etl-mass-batch
## 328 stock numbers, 9094 files

## System Test for PreValidate
if (__name__ == "__main__"):
	import boto3
	from Config import *
	from AWSSession import *

	lptsReader = LPTSExtractReader(os.environ["LPTS_XML"])
	unicodeScript = UnicodeScript()
	preValidate = PreValidate(lptsReader, unicodeScript)
	config = Config.shared()
	bucket = "dbp-etl-mass-batch"
	session = boto3.Session(profile_name = config.s3_aws_profile)
	s3Client = session.client('s3')

	testDataMap = {}
	request = { 'Bucket': bucket, 'MaxKeys': 1000 }
	hasMore = True 
	while hasMore:
		response = s3Client.list_objects_v2(**request)
		hasMore = response['IsTruncated']
		for item in response.get('Contents', []):
			objKey = item.get('Key')
			#print(objKey)
			(directory, filename) = objKey.rsplit("/", 1)
			files = testDataMap.get(directory, [])
			files.append(filename)
			testDataMap[directory] = files
		if hasMore:
			request['ContinuationToken'] = response['NextContinuationToken']

	for (directory, filenames) in testDataMap.items():
		baseDir = os.path.basename(directory)
		stockNumber = preValidate.isTextStockNo(baseDir)
		if stockNumber == None:
			print("ERROR stock no not recognized")
		else:
			oneFilePath = directory + "/" + filenames[0]
			sampleText = unicodeScript.readObject(s3Client, "s3://" + bucket, oneFilePath)
			resultList = preValidate.validateUSXStockNo(stockNumber, filenames, sampleText)
		print("ERRORS", preValidate.messages)
		preValidate.messages = {}


# time python3 load/PreValidate.py newdata
	

