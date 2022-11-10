
from botocore.exceptions import ClientError
from PreValidateResult import *
from UnicodeScript import *

class TextStockNumberProcessor:

	def __init__(self, languageReader):
		self.languageReader = languageReader
		self.unicodeScript = UnicodeScript()
		self.errors = [] # array of strings
		self.OT = { "GEN", "EXO", "LEV", "NUM", "DEU", "JOS", "JDG", "RUT", "1SA", "2SA", "1KI", "2KI", "1CH", "2CH", 
					"EZR", "NEH", "EST", "JOB", "PSA", "PRO", "ECC", "SNG", "ISA", "JER", "LAM", "EZK", "DAN", "HOS", 
					"JOL", "AMO", "OBA", "JON", "MIC", "NAM", "HAB", "ZEP", "HAG", "ZEC", "MAL" }
		self.NT = { "MAT", "MRK", "LUK", "JHN", "ACT", "ROM", "1CO", "2CO", "GAL", "EPH", "PHP", "COL", "1TH", "2TH", 
					"1TI", "2TI", "TIT", "PHM", "HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD", "REV" }			

	def validateTextStockNumbersFromLambda(self, stocknumberFileContentsString, directoryName, filenames):
		resultList = []
		stockNumberList = self.getStockNumberList(stocknumberFileContentsString, directoryName)
		if stockNumberList != None and len(stockNumberList) > 0:
			stockNumberResultList = self.validateUSXStockList(stockNumberList, filenames)
			resultList.extend(stockNumberResultList)
		return (resultList, self.errors)


	def validateTextStockNumbersFromController(self, stocknumberFileContentsString, directoryName, s3Client, location, fullPath):
		resultList = []
		stockNumberList = self.getStockNumberList(stocknumberFileContentsString, directoryName)
		if stockNumberList != None and len(stockNumberList) > 0:
			(filenames, actualScript) = self.getSampleUnicodeTextFromS3(s3Client, location, fullPath)
			stockNumberResultList = self.validateUSXStockList(stockNumberList, filenames, actualScript)
			resultList.extend(stockNumberResultList)
		return (resultList, self.errors)


	def getStockNumberList(self,stocknumberFileContentsString, directoryName):
		if (stocknumberFileContentsString != None and stocknumberFileContentsString != ""):
			stockNumberList = self.parseStockNumberString(stocknumberFileContentsString)
		else:
			stockNumberList = self.parseStockNumberFromDirectoryName(directoryName)
		return stockNumberList
		

	# this is the main validation method for both lambda and ETL, after factoring out the differences in how to read the stocknumber.txt file.
	# the stocknumber[s] are in a string
	# returns: 1) array of PreValidateResult objects; (2) errors
	# def validateTextStockNumbers(self, stocknumberFileContentsString, directoryName, s3Client, location, fullPath):
	# 	resultList = []
	# 	#print ("validateTextStockNumbers... stocknumberFileContentsString ..%s.., directoryName [%s]" %(stocknumberFileContentsString, directoryName))
	# 	# stocknumberString is set by frontend dropzone, only if a stocknumber.txt file exists. Stocknumber string is the contents of that file
	# 	if (stocknumberFileContentsString != None and stocknumberFileContentsString != ""):
	# 		stockNumberList = self.parseStockNumberString(stocknumberFileContentsString)
	# 	else:
	# 		stockNumberList = self.parseStockNumberFromDirectoryName(directoryName)

	# 	if stockNumberList != None and len(stockNumberList) > 0:
	# 		(filenames, actualScript) = self.getSampleUnicodeTextFromS3(s3Client, location, fullPath)

	# 		stockNumberResultList = self.validateUSXStockList(stockNumberList, filenames, actualScript)

	# 		resultList.extend(stockNumberResultList)

	# 	return (resultList, self.errors)

	# returns an array of PreValidateResult objects
	def validateUSXStockList(self, stockList, filenames, actualScript = None):
		resultList = []
		for stockNumber in stockList:
			result = self.validateUSXstockNumber(stockNumber, filenames, actualScript)
			resultList.extend(result)

		return resultList

	# returns an array of PreValidateResult objects
	def validateUSXstockNumber(self, stockNumber, filenames, actualScript = None):
		# print("text processor:validateUSXStockNumber. stockNumber [%s], count of filenames [%s], actualScript [%s] " % (stockNumber, len(filenames), actualScript))
		lptsRecord = self.languageReader.getByStockNumber(stockNumber)
		if lptsRecord == None:
			self.errors.append("stockNumber [%s] is not in LPTS" % (stockNumber))
			return []

		scopeMap = self._findDamIdScopeMap(stockNumber, lptsRecord) # scopeMap is { scope: [(damId, script)] }
		bookIdMap = self._findBookIdMap(stockNumber, filenames) # bookIdMap is { bookId: filename }
		ntResult = self._matchFilesToDamId(stockNumber, "N", lptsRecord, scopeMap, bookIdMap, actualScript) # PreValidateResult or None
		# if ntResult != None:
		# 	print("NT", ntResult.toString())
		otResult = self._matchFilesToDamId(stockNumber, "O", lptsRecord, scopeMap, bookIdMap, actualScript) # PreValidateResult or None
		# if otResult != None:
		# 	print("OT", otResult.toString())
		bothResults = self._combineMultiplePScope(stockNumber, ntResult, otResult)
		# for result in bothResults:
		# 	print("BOTH", result.toString())
		return bothResults

	## This method returns a map { bookId: filename } from input filenames
	def _findBookIdMap(self, stockNumber, filenames):
		result = {}
		for filename in filenames:
			name = filename.split(".")[0]
			bookId = name[-3:]
			if result.get(bookId) != None:
				self.errors.append("Stocknumber [%s] has duplicate book_id %s" % (stockNumber, bookId))
			result[bookId] = filename

		return result

	## This method returns a map { scope: [(damId, script)] } from LPTS for a stock number
	def _findDamIdScopeMap(self, stockNumber, lptsRecord):
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

	## Match files included in 
	def _matchFilesToDamId(self, stockNumber, scope, lptsRecord, scopeMap, bookIdMap, actualScript = None):
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
					self.errors.append("Stocknumber [%s], text is script %s, but there is no damId with that script in scope %s." % (stockNumber, actualScript, scope))
			else:
				# at least for USX, it is common for a directory to contain both OT and NT, so the below would throw an unnecessary error. 
				# this happens when all OT books are found, but this method is called for NT, and vice-versa.
				# this may cause other error situations to go unnoticed. 
				print("info only (but potential error for audio and video)... %s contains a full set of books for %s, but there is no damId with that scope" % (stockNumber, scope,))
		elif len(bookIdsFound) > 0:
			if scopeMap.get("P") != None:
				for (damId, index, script) in scopeMap.get("P"):
					if actualScript == None or self.unicodeScript.matchScripts(actualScript, script):
						filenameList = self._getFilenameList(scope, bookIdsFound, bookIdMap)
						result = PreValidateResult(lptsRecord, damId + "-usx", damId, "text", index, filenameList)
				if result == None:
					self.errors.append("text is script %s, but there is no damId with that script in scope P." % (actualScript,))
			else:
				self.errorMessage("contains some books with scope [%s], but there is no damId with scope P" % (scope))

		return result

	def _combineMultiplePScope(self, stockNumber, ntResult, otResult):
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

	####  lower level helper functions related to reading stocknumber.txt file and converting to standard/usable format ###
	def parseStockNumberString(self, contents):
		stockNumberArray = self.transformStocknumberStringToArray(contents)
		stockNumberArrayFinal = []

		if len(stockNumberArray) > 0:
			for stockNumberItem in stockNumberArray:
				if len(stockNumberItem) > 7:
					stockNumberArrayFinal.append(stockNumberItem[:-3] + "/" + stockNumberItem[-3:])
				else:
					stockNumberArrayFinal.append(stockNumberItem)

		return stockNumberArrayFinal	

	# applies to both
	def transformStocknumberStringToArray(self, contents):
		if (len(contents) == 0):
			return []
		# avoid that \r\n is escaped
		contents = contents.replace("\\r", "\r")
		contents = contents.replace("\\n", "\n")
		stocknumberArray = contents.encode("utf-8").decode("utf-8").splitlines()
		stocknumberArrayFinal = []

		for stocknumber in stocknumberArray:
			if stocknumber != '':
				stocknumberArrayFinal.append(stocknumber)

		return stocknumberArrayFinal		

	def parseStockNumberFromDirectoryName(self, directoryName):
		parts = directoryName.split("_")
		if len(parts) > 2:
			stockNumber = parts[-2]
			if len(stockNumber) > 5:
				stockNumber = stockNumber[:-3] + "/" + stockNumber[-3:]
				return [stockNumber]
			else:
				self.errors.append("Could not parse stocknumber from directory name: [%s]" % (directoryName))
		return None


# Note: these are specific to the ETL entry point
	def getStockNumberStringFromS3(self, s3Client, location, fullPath):
		bucket = location.replace("s3://", "")
		key = fullPath + "/stocknumber.txt"

		try:
			response = s3Client.get_object(Bucket=bucket, Key=key)
			contents = response['Body'].read()
			contents = contents.decode("utf-8")
			return contents

		except ClientError as ex:
			if ex.response['Error']['Code'] == 'NoSuchKey':			
				print("No stocknumber.txt file found. bucket [%s], fullPath: [%s], key [%s]" % (bucket, fullPath, key))
		except Exception as err:
			print("Error retrieving stocknumber.txt file: %s" % (err))


	def getSampleUnicodeTextFromS3(self, s3Client, bucket, fullPath):
		filenames = self.unicodeScript.getFilenames(s3Client, bucket, fullPath)
		sampleText =  self.unicodeScript.readObject(s3Client, bucket, fullPath + "/" + filenames[0]) if len(filenames) > 0 else None
		if sampleText != None:
			textArray = self.unicodeScript.parseXMLStrings(sampleText)
			#print("sample", "".join(textArray[:40]))
			(actualScript, matchPct) = self.unicodeScript.findScript(textArray)
		else:
			actualScript = None
		return (filenames, actualScript)