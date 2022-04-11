# PreValidate

# This program has two entrypoints:
# 1) it is called by an AWS lambda handler to verify metadata and configuration is correct before uploading to S3;
# 2) it is also called during DBPLoadController processing

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from LanguageReader import *
from UnicodeScript import *
from botocore.exceptions import ClientError

class PreValidateResult:

	def __init__(self, lptsRecord, filesetId, damId, typeCode, index, fileList = None):
		self.lptsRecord = lptsRecord
		self.filesetId = filesetId
		self.damId = damId
		self.typeCode = typeCode
		self.index = index
		self.fileList = fileList

	def bibleId(self):
		return self.lptsRecord.DBP_EquivalentByIndex(self.index)

	def scope(self):
		return self.damId[6]

	def script(self):
		return self.lptsRecord.Orthography(self.index)

	def toString(self):
		results = []
		results.append("out: %s/%s/%s is %s %d" % (self.typeCode, self.bibleId(), self.filesetId, self.damId, self.index))
		if self.fileList != None:
			for file in self.fileList:
				results.append(file)
		return ", ".join(results)		


class PreValidate:

	def __init__(self, languageReader, unicodeScript, s3Client, bucket):
		self.languageReader = languageReader
		self.unicodeScript = unicodeScript
		self.messages = {}
		self.OT = { "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", 
					"EZR", "NEH", "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", 
					"JOL", "AMO", "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL" }
		self.NT = { "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH", "PHP", "COL", "1TH", "2TH", 
					"1TI", "2TI", "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV" }
		self.s3Client = s3Client
		self.bucket = bucket

	## ENTRY POINT FOR LAMBDA  - validate input and return an errorList.
	def validateLambda(self, languageReader, directory, filenames):
		unicodeScript = UnicodeScript()
		result = None
		stockNumber = self.isTextStockNo(directory)
		if stockNumber != None:
			stockResultList = self.validateUSXStockList(stockNumber, filenames)
			resultList = []

			for stockResult in stockResultList:
				for result in stockResult:
					resultList.append(result)

			if len(resultList) > 0:
				self.validateLPTS(resultList[0])
		else:
			result = self.validateFilesetId(directory)
			self.validateLPTS(result)

		self.addErrorMessages(directory, unicodeScript.errors)
		return self.formatMessages()


	# ENTRY POINT for DBPLoadController processing - 
	def validateDBPELT(languageReader, s3Client, location, directory, fullPath):

		# For audio and video, the directory name contains an embedded fileset id (eg ENGESVN2DA)
		# For text, stocknumber is used instead of filesetid. Usually, the directory name contains an embedded stocknumber (Abidji_N2ABIWBT_USX). 
		# But in some cases, the same USX applies to multiple stocknumbers. When this occurs, the directory contains a file
		# called stocknumber.txt, which lists each associated stocknumber.

		unicodeScript = UnicodeScript()
		bucket = location.replace("s3://", "")
		preValidate = PreValidate(languageReader, unicodeScript, s3Client, bucket)
		resultList = []
		stockNumber = preValidate.isTextStockNo(directory)
		if stockNumber != None:
			filenames = unicodeScript.getFilenames(s3Client, location, fullPath)
			sampleText =  unicodeScript.readObject(s3Client, location, fullPath + "/" + filenames[0]) if len(filenames) > 0 else None
			stockResultList = preValidate.validateUSXStockList(stockNumber, filenames, sampleText)

			for stockResult in stockResultList:
				for result in stockResult:
					resultList.append(result)
		else:
			filesetId = preValidate.parseFilesetIdFromDirectoryName(directory)			
			result = preValidate.validateFilesetId(filesetId)
			if result != None:
				resultList = [result]
		if len(resultList) > 0:
			preValidate.validateLPTS(resultList[0])
		preValidate.addErrorMessages(directory, unicodeScript.errors)
		return (resultList, preValidate.messages)


	## determine if directory is USX text stockNo
	def isTextStockNo(self, directory):
		stockNumberArray = self.isTextStockNoFromFile(directory)

		if len(stockNumberArray) > 0:
			return stockNumberArray

		parts = directory.split("_")
		if len(parts) > 2:
			stockNo = parts[-2]
			if len(stockNo) > 5:
				stockNumber = stockNo[:-3] + "/" + stockNo[-3:]
				return [stockNumber]
			else:
				self.errorMessage(directory, "Could not find a stock no.")
		return None

# determine if stocknumber.txt file exists, and process if so
	def getStockNumbersFromFile(self, directory, bucket = None):
		prefix = directory
		if bucket == None:
			bucket = self.bucket

		key = prefix + "/stocknumber.txt"
		stocknumberArray = []
		stocknumberArrayFinal = []	

		try:
			response = self.s3Client.get_object(Bucket=bucket, Key=key)
			contents = response['Body'].read()
			stocknumberArray = contents.decode("utf-8").splitlines()

			for stocknumber in stocknumberArray:
				if stocknumber != '':
					stocknumberArrayFinal.append(stocknumber)

		except ClientError as ex:
			if ex.response['Error']['Code'] == 'NoSuchKey':			
				print("No stocknumber.txt file found")
		except Exception as err:
			print("Error retrieving stocknumber.txt file: %s" % (err))

		return stocknumberArrayFinal

	def isTextStockNoFromFile(self, directory):
		stockNumberArray = self.getStockNumbersFromFile(directory)

		stockNumberArrayFinal = []

		if len(stockNumberArray) > 0:
			for stockNumberItem in stockNumberArray:
				stockNumberArrayFinal.append(stockNumberItem[:-3] + "/" + stockNumberItem[-3:])

		return stockNumberArrayFinal




	def validateUSXStockList(self, stockList, filenames, sampleText = None):
		resultList = []
		for stockListNumber in stockList:
			result = self.validateUSXStockNo(stockListNumber, filenames, sampleText)
			resultList.append(result)
		return resultList

	def parseFilesetIdFromDirectoryName(self, directory):
		return directory[:7] + "_" + directory[8:] + "-usx" if directory[8:10] == "ET" else directory

	## Validate filesetId and return PreValidateResult
	def validateFilesetId(self, filesetId):
		filesetId1 = filesetId.split("-")[0]
		damId = filesetId1.replace("_", "2")
		results = self.languageReader.getFilesetRecords10(damId) # This method expects 10 digit DamId's always
		if results == None:
			damId = filesetId1.replace("_", "1")
			results = self.languageReader.getFilesetRecords10(damId)
			if results == None:
				self.errorMessage(filesetId1, "filesetId is not in LPTS")
				return None
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
		lptsRecord = self.languageReader.getByStockNumber(stockNo)
		if lptsRecord == None:
			self.errorMessage(stockNo, "stockNum  (read from stockNumber.txt [%s]) is not in LPTS" % (stockNo))
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
		# if ntResult != None:
		# 	print("NT", ntResult.toString())
		otResult = self._matchFilesToDamId(stockNo, "O", lptsRecord, scopeMap, bookIdMap, actualScript) # PreValidateResult or None
		# if otResult != None:
		# 	print("OT", otResult.toString())
		bothResults = self._combineMultiplePScope(stockNo, ntResult, otResult)
		# for result in bothResults:
		# 	print("BOTH", result.toString())
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
						filenameList = self._getFilenameList(scope, bookIdsFound, bookIdMap)
						result = PreValidateResult(lptsRecord, damId + "-usx", damId, "text", index, filenameList)
				if result == None:
					self.errorMessage(stockNo, "text is script %s, but there is no damId with that script in scope %s." % (actualScript, scope))
			else:
				# at least for USX, it is common for a directory to contain both OT and NT, so the below would throw an unnecessary error. 
				# this happens when all OT books are found, but this method is called for NT, and vice-versa.
				# this may cause other error situations to go unnoticed. 
				print("info only (but potential error for audio and video)... %s contains a full set of books for %s, but there is no damId with that scope" % (stockNo, scope,))
		elif len(bookIdsFound) > 0:
			if scopeMap.get("P") != None:
				for (damId, index, script) in scopeMap.get("P"):
					if actualScript == None or self.unicodeScript.matchScripts(actualScript, script):
						filenameList = self._getFilenameList(scope, bookIdsFound, bookIdMap)
						result = PreValidateResult(lptsRecord, damId + "-usx", damId, "text", index, filenameList)
				if result == None:
					self.errorMessage(stockNo, "text is script %s, but there is no damId with that script in scope P." % (actualScript,))
			else:
				self.errorMessage(stockNo, "contains some books, but there is no damId with scope P")
		return result


	def _getFilenameList(self, nToT, selectBookIds, bookIdMap):
		result = []
		if nToT == "N":
			includeBooks = selectBookIds
		else:
			includeBooks = set(bookIdMap.keys()).difference(self.NT)
		for bookId in includeBooks:
			filename = bookIdMap[bookId]
			result.append(filename)
		return sorted(result)


	def _combineMultiplePScope(self, stockNo, ntResult, otResult):
		results = []
		if ntResult != None and ntResult.scope() == "P" and otResult != None and otResult.scope() == "P":
			fullFiles = otResult.fileList + ntResult.fileList
			ntResult.fileList = fullFiles
			results.append(ntResult)
			return results
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

	if len(sys.argv) < 3:
		print("FATAL command line parameters: environment from_text_processing_folder ")
		sys.exit()

	prefix = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
	config = Config.shared()
	languageReader = LanguageReaderCreator().create(config)
	unicodeScript = UnicodeScript()
	bucket = "etl-development-input"
	session = boto3.Session(profile_name = config.s3_aws_profile)
	s3Client = session.client('s3')
	preValidate = PreValidate(languageReader, unicodeScript, s3Client, bucket)

	testDataMap = {}
	request = { 'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1000 }
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
		print(directory)
		baseDir = os.path.basename(directory)
		stockNumber = preValidate.isTextStockNo(baseDir)
		if stockNumber == None:
			print("ERROR stock no not recognized")
		else:
			oneFilePath = directory + "/" + filenames[0]
			sampleText = unicodeScript.readObject(s3Client, "s3://" + bucket, oneFilePath)
			stockResultList = preValidate.validateUSXStockList(stockNumber, filenames, sampleText)
			for stockResult in stockResultList:
				for result in stockResult:
					print(result.toString())
		print("ERRORS", preValidate.messages)
		preValidate.messages = {}


# time python3 load/PreValidate.py test
	

