# PreValidate

# This program has two entrypoints:
# 1) it is called by an AWS lambda handler to verify metadata and configuration is correct before uploading to S3;
# 2) it is also called during DBPLoadController processing

# The main of this class expects an environment variable $LPTS_XML set to the full path to that XML file.

import json
from PreValidateResult import *
from LanguageReader import *
from TextStockNumberProcessor import *

class PreValidate:

	def __init__(self, languageReader, s3Client, bucket):
		self.languageReader = languageReader
		self.messages = {}	
		self.s3Client = s3Client
		self.bucket = bucket		


	## ENTRY POINT FOR LAMBDA  - validate input and return an errorList. 
	# stocknumberString is a string representation of the contents of the stocknumber.txt file, if it exists.
	# there is no bucket involved, since the code has not been uploaded yet
	def validateLambda(self, directoryName, filenames, stocknumberFileContentsString = None):
		result = None
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

		(stockNumberResultList, textProcessingMessages) = textStockNumberProcessor.validateTextStockNumbersFromController(stocknumberString, directoryName, s3Client, location, fullPath)
		self.addErrorMessages("text-processing", textProcessingMessages)
		if (self.hasErrors()):
			return ([], self.messages)

		if len(stockNumberResultList) > 0:
			self.validateLPTS(stockNumberResultList[0])
			resultList.extend(stockNumberResultList)
		else:
			result = self.validateFilesetId(directoryName)
			if (result != None):
				self.validateLPTS(result)
				resultList = [result]

		return (resultList, self.messages)


	## Validate filesetId and return PreValidateResult
	def validateFilesetId(self, directoryName):
		filesetId = directoryName
		filesetId1 = directoryName.split("-")[0]
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
		for (languageRecord, status, fieldName) in results:
			stockNum = languageRecord.Reg_StockNumber()
			if stockNum != None:
				stockNumSet.add(stockNum)
			damId = languageRecord.record.get(fieldName)

			dbpFilesetId = filesetId
			if "Audio" in fieldName:
				media = "audio"
			elif "Text" in fieldName:
				media = "text"	
				# for the case when text (which is usx) is loaded from a directory containing the filesetid:
				# we know the text is actually usx, so here is where we want to change the filesetid to include the suffix -usx
				# FIXME: add -usx to filesetid, but only if not already present		
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
			return PreValidateResult(languageRecord, filesetId, damId, list(mediaSet)[0], index)
		else:
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
		if pre.languageRecord.Licensor() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Licensor")
		if pre.languageRecord.Reg_StockNumber() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Reg_StockNumber")
		if pre.languageRecord.Volumne_Name() == None:
			self.requiredFields(pre.filesetId, stockNumber, "Volumne_Name")

		if pre.typeCode == "text" and pre.languageRecord.Orthography(pre.index) == None:
			fieldName = "_x003%d_Orthography" % (pre.index)
			self.requiredFields(pre.filesetId, stockNumber, fieldName)


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
	import boto3
	from Config import *
	from AWSSession import *
	from LanguageReaderCreator import LanguageReaderCreator	

	if len(sys.argv) < 2:
		print("FATAL command line parameters: environment location prefix directoryName")
		sys.exit()

	location = "s3://dbp-etl-upload-dev-ya1mbvdty8tlq15r"
	prefix = "2022-03-25-16-14-34"
	directoryName = "Abidji_N2ABIWBT_USX"
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE") # Should be "B" or "C"

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
	languageReader = LanguageReaderCreator(migration_stage).create(config.filename_lpts_xml)
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
