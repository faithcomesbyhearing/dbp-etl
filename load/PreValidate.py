# PreValidate

# This program has two entrypoints:
# 1) it is called by an AWS lambda handler to verify metadata and configuration is correct before uploading to S3;
# 2) it is also called during DBPLoadController processing

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import re
from PreValidateResult import *
from LanguageReader import *
from TextStockNumberProcessor import *

class PreValidate:

	def __init__(self, languageReader=None, s3Client=None, bucket=""):
		self.languageReader = languageReader
		self.messages = {}	
		self.s3Client = s3Client
		self.bucket = bucket		


	## ENTRY POINT FOR LAMBDA  - validate input and return an errorList. 
	# stocknumberString is a string representation of the contents of the stocknumber.txt file, if it exists.
	# there is no bucket involved, since the code has not been uploaded yet
	def validateLambda(self, directoryName, filenames, stocknumberFileContentsString = None):
		result = None
		# TextStockNumberProcessor will be invoked but the variable self.languageReader could be None
		textStockNumberProcessor = TextStockNumberProcessor(self.languageReader)
		(stockNumberResultList, textProcessingErrors) = textStockNumberProcessor.validateTextStockNumbersFromLambda(stocknumberFileContentsString, directoryName, filenames)
		self.addErrorMessages("text-processing", textProcessingErrors)
		if (self.hasErrors()):
			return self.formatMessages()

		if len(stockNumberResultList) > 0:
				self.validateLPTS(stockNumberResultList[0])
		else:
			# this isn't text; it's audio or video, which is provided as a single fileset
			result = self.validateFilesetId(directoryName)
			if (result != None):
				self.validateLPTS(result)

		return self.formatMessages() # don't change - dbp-etl-web expects this


	# ENTRY POINT for DBPLoadController processing - 
	def validateDBPETL(self, s3Client, location, directoryName, fullPath):
		# entry to validateDBPETL.. location [s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r], directory [French_N1 & O1 FRABIB_USX], fullPath [2022-04-15-17-03-50/French_N1 & O1 FRABIB_USX]
		resultList = []

		# note: s3Client and location are passed in to specific methods instead of to the constructor, since these
		# objects are not relevant for the lambda entry point
		textStockNumberProcessor = TextStockNumberProcessor(self.languageReader)
		stocknumberString = textStockNumberProcessor.getStockNumberStringFromS3(s3Client, location, fullPath)
		print("PreValidate.validateDBPETL. after call to getStocknumberStringFromS3. stocknumberString: %s " % (stocknumberString))
		(stockNumberResultList, textProcessingMessages) = textStockNumberProcessor.validateTextStockNumbersFromController(stocknumberString, directoryName, s3Client, location, fullPath)
		self.addErrorMessages("text-processing", textProcessingMessages)
		if (self.hasErrors()):
			return ([], self.messages)

		if len(stockNumberResultList) > 0:
			print("PreValidate.validateDBPETL. line 62. stocknumberResultList > 0.. value: %s" % (stockNumberResultList[0]))
			self.validateLPTS(stockNumberResultList[0])
			resultList.extend(stockNumberResultList)
		else:
			print("PreValidate.validateDBPETL. line 66. stocknumberResultList  was not >0.. validate filesetid from directoryName: %s" % (directoryName))
			result = self.validateFilesetId(directoryName)
			print("PreValidate.validateDBPETL. result from validateFilesetId: %s" % (result))
			if (result != None):
				print("PreValidate.validateDBPETL. result from validateFilesetId was not empty, so validateLPTS ???")
				self.validateLPTS(result)
				resultList = [result]

				parts = directoryName.split("_")
				if len(parts) > 2 and parts[0].lower() == "covenant":
					bucket = location[5:]

					request = { 'Bucket': bucket, 'MaxKeys': 1000, 'Prefix': fullPath + "/" }
					response = s3Client.list_objects_v2(**request)
					pattern = re.compile(r'^COVENANT_SEGMENT\s*\d{1,2}[A-Z]?\s*.*\.mp4$', re.IGNORECASE)

					covenentFiles = []

					for item in response.get('Contents', []):
						objKey = item.get('Key')
						filename = objKey[len(fullPath) + 1:]

						if pattern.match(filename):
							covenentFiles.append(filename)

					if len(covenentFiles) == 0:
						self.errorMessage(directoryName, "ERROR: Convenant files does not have the valid format in bucket %s or prefix %s/" % (bucket, fullPath))

		return (resultList, self.messages)

	def calculateCovenantFileset(self, stocknumber):
		# Find the position of the '/'
		slash_index = stocknumber.find('/')

		# Check if the '/' is found and the index 2 is within the bounds
		if slash_index != -1 and len(stocknumber) > 2:
			return stocknumber[2:slash_index]+stocknumber[slash_index+1:]+"S2DV"

		return None

	## Validate filesetId and return PreValidateResult
	def validateFilesetId(self, directoryName):
		# directoryName is the name of the directory, which is the filesetId
		filesetId = directoryName
		filesetId1 = directoryName.split("-")[0]
		print("PreValidate.validateFilesetId. directoryName: %s, filesetId: %s, filesetId1: %s " % (directoryName, filesetId, filesetId1))
		damId = filesetId1.replace("_", "2")
		results = self.languageReader.getFilesetRecords10(damId) # This method expects 10 digit DamId's always
		print("PreValidate.validateFilesetId. damId: %s check1: results: %s" % (damId, results))
		if results == None:
			damId = filesetId1.replace("_", "1")
			results = self.languageReader.getFilesetRecords10(damId)
			print("PreValidate.validateFilesetId. check2: results: %s" % (results))

			if results == None:
				parts = directoryName.split("_")
				# It supports the case when the directory name is example: "Covenant_Manobo, Obo SD_S2OBO_COV"
				if len(parts) > 2 and parts[0] == "Covenant":
					# check if the directory name is a covenant stock number
					covenantStocknumber = directoryName[-9:]

					# Check if the fourth character is an underscore
					if covenantStocknumber[5] == '_':
						covenantStocknumber = covenantStocknumber.replace('_', '/')
						existsStocknumber = self.languageReader.getStocknumber(covenantStocknumber)

						if existsStocknumber != None:
							covenentFileset = self.calculateCovenantFileset(covenantStocknumber)
							filesetId = covenentFileset
							results = self.languageReader.getFilesetRecords10(covenentFileset)
						else:
							self.errorMessage(existsStocknumber, "Stocknumber is not in Biblebrain")
							return None

				if results == None:
					self.errorMessage(filesetId1, "filesetId is not in Biblebrain")
					return None

		stockNumSet = set()
		mediaSet = set()
		bibleIdSet = set()
		for (languageRecord, _, fieldName) in results:
			print("PreValidate.validateFilesetId. didn't return error.. languageRecord: %s, fieldName: %s" % (languageRecord, fieldName))
			if not languageRecord.IsActive():
				print("PreValidate.validateFilesetId.  languageRecord not active.. continue: %s" % (languageRecord))
				continue
			stockNum = languageRecord.Reg_StockNumber() 
			print("PreValidate.validateFilesetId. regStockNumber: %s" % (stockNum))
			if stockNum != None:
				stockNumSet.add(stockNum)
			damId = languageRecord.record.get(fieldName)
			print("PreValidate.validateFilesetId. damId: %s" % (damId))
			if "Audio" in fieldName:
				media = "audio"
			elif "Text" in fieldName:
				media = "text"	
				# for the case when text (which is usx) is loaded from a directory containing the filesetid,
				# we know the text is actually usx, so, we should make sure that filesetid includes the suffix -usx
				print("PreValidate.validateFilesetId. damId: %s" % (damId))
				if not filesetId.endswith("-usx"):
					print("PreValidate.validateFilesetId. filesetId doesn't end with -usx, so adding -usx: %s" % (filesetId))
					filesetId = filesetId + "-usx"
			elif "Video" in fieldName:
				media = "video"
			else:
				media = "unknown"
				self.errorMessage(filesetId, "in %s does not have Audio, Text or Video in DamId fieldname." % (stockNum,))
			mediaSet.add(media)

			if "3" in fieldName:
				index = 3
			elif "2" in fieldName:
				index = 2
			else:
				index = 1

			bibleId = languageRecord.DBP_EquivalentByIndex(index)
			print("PreValidate.validateFilesetId. index: %s, bibleId: %s" % (index, bibleId))
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
			print("PreValidate.validateFilesetId. mediaSet and bibleIdSet not empty. return PreValidateResult..")
			return PreValidateResult(languageRecord, filesetId, damId, list(mediaSet)[0], index)
		else:
			print("PreValidate.validateFilesetId. mediaSet and bibleIdSet empty. return None")
			return None

	def validateLPTS(self, preValidateResult):
		pre = preValidateResult
		stockNumber = pre.languageRecord.Reg_StockNumber()
		if pre.languageRecord.Copyrightc() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Copyrightc")
		if pre.typeCode in {"audio", "video"} and pre.languageRecord.Copyrightp() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Copyrightp")
		if pre.typeCode == "video" and pre.languageRecord.Copyright_Video() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Copyright_Video")
		if pre.languageRecord.ISO() == None:
			self.requiredFields(pre.filesetId, stockNumber, "ISO")
		if pre.languageRecord.LangName() == None:
			self.requiredFields(pre.filesetId, stockNumber, "LangName")
		if pre.languageRecord.LicensorList() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Licensor")
		if pre.languageRecord.Reg_StockNumber() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Reg_StockNumber")
		if pre.languageRecord.Volumne_Name() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Volumne_Name")


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


	def requiredFields(self, filesetId, stockNumber, fieldName):
		message = "LPTS %s field %s is required." % (stockNumber, fieldName)
		self.errorMessage(filesetId, message)

	# returns array of Strings
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

## System Test for PreValidate,testing the validateDBPETL entry point (see PreValidateLambdaHandlerStub for the lambda entry point system test)
if (__name__ == "__main__"):
	import sys
	from Config import Config
	from AWSSession import AWSSession
	from LanguageReaderCreator import LanguageReaderCreator	

	if len(sys.argv) < 2:
		print("FATAL command line parameters: environment location prefix directoryName")
		sys.exit()

	location = "s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r"
	prefix = "2022-03-25-16-14-34"
	directoryName = "Abidji_N2ABIWBT_USX"
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B")

	if len(sys.argv) > 2:
		location = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
	
	if len(sys.argv) > 3:
		prefix = sys.argv[3]

	if len(sys.argv) > 4:
		directoryName = sys.argv[4]

	if len(sys.argv) > 5:
		migration_stage = sys.argv[5]

	print ("location [%s], prefix [%s], directoryName [%s] migration [%s]" % (location, prefix, directoryName, migration_stage))
	fullPath = prefix + "/" + directoryName if prefix != "" else directoryName

	config = Config()
	AWSSession.shared()
	lpts_xml = config.filename_lpts_xml if migration_stage == "B" else ""
	languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)
	s3Client = AWSSession.shared().s3Client

	preValidate = PreValidate(languageReader, s3Client, location) 
	(resultList, messages) = preValidate.validateDBPETL(s3Client, location, directoryName, fullPath)
	if (len(messages) > 0):
		print ("validate failed: %s" % (messages))
	else:
		print("Validation passed")


# Note: see Lambda Handler stub for testing the lambda entry point
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-03-25-16-14-34 Abidji_N2ABIWBT_USX
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-03-25-16-14-34 Abidji_N2ABIWxx_USX
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-04-14-21-05-56 "Kannada_N1 & O1 KANDPI_USX"
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-04-14-21-05-56 ENGESVN1DA

# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-03-25-16-14-34 Spanish_N2SPNTLA_USX
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2022-09-27-13-48-13 ENGESVO2ET

# dbp-etl-dev s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r/2022-10-17-18-49-35/Spanish_N2SPNTLA_USX/

# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2023-07-10-15-38-20 JAAJARN2DA/
# python3 load/PreValidate.py test s3://etl-development-input/ "" TGKWBTP2DV
# python3 load/PreValidate.py test s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r 2024-07-24-17-15-56 Spanish_N2SPNBDA_USX
# python3 load/PreValidate.py test s3://etl-development-input/ "" "Covenant_Manobo, Obo SD_S2OBO_COV"
# python3 load/PreValidate.py test s3://etl-development-input/ "" ACECOVS2DV
