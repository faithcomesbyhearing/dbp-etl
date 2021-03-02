# PreValidate

# This program is used as an AWS lambda to verify filesets that are to be uploaded.
# It is also called by the primary Validate.py program during the actual run.


class PreValidate:


	def __init__(self, lptsReader):
		self.lptsReader = lptsReader 


	def validateFilesetId(self, filesetId):
		logger = Log.getLogger(filesetId)
		results = self.lptsReader.getFilesetRecords10(filesetId)
		if results == None:
			logger.message(Log.EROR, "is not in LPTS" % ())
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
					logger.message(Log.EROR, "in %s does not have Audio, Text, or Video in DamId fieldname." % (stockNum,))
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
				logger.message(Log.EROR, "in %s has more than one media type: %s" % (", ".join(stockNumSet), ", ".join(mediaSet)))
			if len(bibleIdSet) == 0:
				logger.message(Log.EROR, "in %s does not have a DBP_Equivalent" % (", ".join(stockNumSet)))
			if len(bibleIdSet) > 1:
				logger.message(Log.EROR, "in %s has more than one DBP_Equivalent: %s" % (", ".join(stockNumSet), ", ".join(bibleIdSet)))

			return (list(mediaSet)[0], list(bibleIdSet)[0])


	def validateLPTS(self, typeCode, bibleId, filesetId):
		logger = Log.getLogger3(typeCode, bibleId, filesetId)
		results = self.lptsReader.getFilesetRecords10(filesetId)
		(lptsRecord, index) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
		if results == None:
			logger.message(Log.EROR, "is not in LPTS" % ())
			return None
		else:
			for (lptsRecord, status, damIdField) in results:
				stockNo = lptsRecord.Reg_StockNumber()
				if lptsRecord.Copyrightc() == None:
					logger.requiredFields(stockNo, "Copyrightc")
				if lptsRecord.Copyrightp() == None:
					logger.requiredFields(stockNo, "Copyrightp")
				if lptsRecord.Copyright_Video() == None:
					logger.requiredFields(stockNo, "Copyright_Video")
				if lptsRecord.ISO() == None:
					logger.requiredFields(stockNo, "ISO")
				if lptsRecord.LangName() == None:
					logger.requiredFields(stockNo, "LangName")
				if lptsRecord.Licensor() == None:
					logger.requiredFields(stockNo, "Licensor")
				if lptsRecord.Reg_StockNumber() == None:
					logger.requiredFields(stockNo, "Reg_StockNumber")
				if lptsRecord.Volumne_Name() == None:
					logger.requiredFields(stockNo, "Volumne_Name")

				if typeCode == "text":
					if "3" in damIdField:
						index = 3
					elif "2" in damIdField:
						index = 2
					else:
						index = 1

					if lptsRecord.Orthography(index) == None:
						fieldName = "_x003%d_Orthography" % (index)
							logger.requiredFields(stockNo, fieldName)
						
					scriptName = lptsRecord.Orthography(index)
					if scriptName != None and scriptName not in self.scriptNameSet:
						logger.invalidValues(stockNo, "_x003n_Orthography", scriptName)

					numeralsName = lptsRecord.Numerals()
					if numeralsName != None and numeralsName not in self.numeralsSet:
						logger.invalidValues(stockNo, "Numerals", numeralsName)


if (__name__ == '__main__'):
	config = Config.shared() ############## change this to be an environment variable, and pass value into Reader
	lptsReader = LPTSExtractReader(config)
	validate = PreValidate(lptsReader)

	data = json.load(sys.stdin)	
	#print(data)
	print(data.get("prefix"))
	filesetId = data.get("prefix")
	prefix = validate.validateFilesetId(filesetId)
	if prefix != None and Log.totalErrorCount() == 0:
		(typeCode, bibleId) = prefix
		print(typeCode, bibleId)
		validate.validateLPTS(typeCode, bibleId, filesetId)
	Log.printLog()

