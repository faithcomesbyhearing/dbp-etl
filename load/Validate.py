# Validate.py

import os
import sys
import zipfile
from datetime import datetime
from Config import *
from SQLUtility import *
from InputReader import *
from LPTSExtractReader import *
from FilenameParser import *
from FilenameReducer import *


class Validate:

	def parseCommandLine():
		if len(sys.argv) != 3:
			print("Usage: python3 py/Validate.py  config_profile  run_type")
			print("\tconfig_profile: e.g. dev, test, stage, prod")
			print("\trun_type: files, bucketlists")
			sys.exit()
		elif not sys.argv[2] in {"files", "bucketlists"}:
			print("2nd parameter must be files or bucketlists")
			sys.exit()
		elif not sys.argv[1] in {"dev", "test", "stage", "prod"}:
			print("1st parameter is expected to be dev, test, stage, prod")
		results = {}
		results["config"] = sys.argv[1]
		results["run"] = sys.argv[2]
		return results
			

	def __init__(self, runType, config, db, lptsReader):
		self.config = config
		self.db = db
		self.runType = runType
		self.lptsReader = lptsReader
		self.errorMessages = []
		self.invalidFileExt = []
		self.missingBibleIds = []
		self.missingFilesetIds = []
		self.requiredFields = []
		self.suggestedFields = []
		self.invalidValues = []
		self.damIdStatus = []
		self.scriptNameSet = db.selectSet("SELECT lpts_name FROM lpts_script_codes", ())
		self.numeralsSet = db.selectSet("SELECT id FROM numeral_systems", ())



	def process(self):
		self.prepareDirectory(self.config.directory_accepted)
		self.prepareDirectory(self.config.directory_quarantine)
		self.prepareDirectory(self.config.directory_duplicate)
		self.prepareDirectory(self.config.directory_errors)

		## get filesets: a map typeCode/bibleId/filesetId: [(filename, fileSize, datetime)]
		reader = InputReader(self.config)
		if self.runType == "bucketlists":
			filesets = reader.bucketListing(self.config.s3_bucket)
			filesets = reader.bucketListing(self.config.s3_vid_bucket)

		elif self.runType == "files":
			filesets = reader.fileListing(self.config.directory_validate)

		else:
			print("ERROR: run_type must be files or bucketlists.")
			sys.exit()

		## Validate filetypes
		for (filePrefix, filenames) in filesets.items():
			(typeCode, bibleId, filesetId) = filePrefix.split("/")
			for (filename, filesize, lastmodified) in filenames:
				ext = os.path.splitext(filename)[1]
				if typeCode == "audio" and ext != ".mp3":
					self.invalidFileExt.append((typeCode, bibleId, filesetId, filename))
				elif typeCode == "text" and not ext in {".html", ".json"}:
					self.invalidFileExt.append((typeCode, bibleId, filesetId, filename))
				elif typeCode == "video" and ext != ".mp4":
					self.invalidFileExt.append((typeCode, bibleId, filesetId, filename))



		## Reduce input files to typeCode/bibleId: [filesetId]
		inputIdMap = {}
		for filePrefix in filesets.keys():
			(typeCode, bibleId, filesetId) = filePrefix.split("/")
			key = typeCode + "/" + bibleId
			filesetIdList = inputIdMap.get(key, [])
			filesetIdList.append(filesetId)
			inputIdMap[key] = filesetIdList

		self.validateLPTS(inputIdMap)

		FilenameReducer.openAcceptErrorSet(self.config)
		parser = FilenameParser(self.config)
		parser.process3(filesets, self.lptsReader, self.errorMessages)
		#parser.summary3()

		find = FindDuplicateFilesets(self.config)
		duplicates = find.findDuplicates()
		find.moveDuplicates(duplicates, self.errorMessages)


	## prepareDirectory 1. Makes sure a directory exists. 2. If it contains .csv files,
	## they are packaged up into a zip file using the timestamp of the first csv file.
	def prepareDirectory(self, directory):
		if not os.path.isdir(directory):
			os.makedirs(directory)
		else:
			modifiedTime = self.getModifiedTime(directory)
			if modifiedTime != None:
				pattern = self.config.filename_datetime 
				zipfilePath = directory + modifiedTime.strftime(pattern) + ".zip"

				zipDir = zipfile.ZipFile(zipfilePath, "w")
				with zipDir:
					for file in os.listdir(directory):
						if file.endswith("csv") and not file.startswith("._"):
							fullPath = directory + os.sep + file
							zipDir.write(fullPath, file)
							os.remove(fullPath)
 

	def getModifiedTime(self, directory):
		listDir = os.listdir(directory)
		if len(listDir) > 0:
			for file in listDir:
				if file.endswith("csv"):
					filePath = directory + os.sep + file
					filetime = os.path.getmtime(filePath)
					return datetime.fromtimestamp(filetime)
		return None


	def validateLPTS(self, inputIdMap):
		for biblePrefix in sorted(inputIdMap.keys()):
			(typeCode, bibleId) = biblePrefix.split("/")

			## Validate bibleId agains LPTS
			lptsRecords = self.lptsReader.bibleIdMap.get(bibleId)
			if lptsRecords == None:
				self.missingBibleIds.append((typeCode, bibleId))
			else:

				## Validate filesetIds against LPTS
				filesetIds = inputIdMap[biblePrefix]
				for filesetId in filesetIds:
					(lptsRecord, index) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
					if lptsRecord == None:
						self.missingFilesetIds.append((typeCode, bibleId, filesetId))
					else:

						stockNo = lptsRecord.Reg_StockNumber()
						#if not status in {"Live", "live"}:
						#	self.damIdStatus.append((typeCode, bibleId, filesetId, stockNo, status))

						## Validate Required fields are present
						if lptsRecord.Copyrightc() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Copyrightc"))
						if lptsRecord.Copyrightp() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Copyrightp"))
						if lptsRecord.Copyright_Video() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Copyright_Video"))
						if lptsRecord.ISO() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "ISO"))
						if lptsRecord.LangName() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "LangName"))
						if lptsRecord.Licensor() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Licensor"))
						if lptsRecord.Reg_StockNumber() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Reg_StockNumber"))
						if lptsRecord.Volumne_Name() == None:
							self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, "Volumne_Name"))

						if typeCode == "text":
							if lptsRecord.Orthography(index) == None:
								fieldName = "_x003%d_Orthography" % (index)
								self.requiredFields.append((typeCode, bibleId, filesetId, stockNo, fieldName))
						
						scriptName = lptsRecord.Orthography(index)
						if scriptName != None and scriptName not in self.scriptNameSet:
							self.invalidValues.append((typeCode, bibleId, filesetId, stockNo, "_x003n_Orthography", scriptName))

						numeralsName = lptsRecord.Numerals()
						if numeralsName != None and numeralsName not in self.numeralsSet:
							self.invalidValues.append((typeCode, bibleId, filesetId, stockNo, "Numerals", numeralsName))

	def reportErrors(self):
		for (typeCode, bibleId, filesetId, filename) in self.invalidFileExt:
			self.errorMessages.append("%s/%s/%s/%s has an invalid file ext.\tEROR" % (typeCode, bibleId, filesetId, filename))
		for (typeCode, bibleId) in self.missingBibleIds:
			self.errorMessages.append("%s/%s bibleId is not in LPTS.\tEROR" % (typeCode, bibleId,))
		for (typeCode, bibleId, filesetId) in self.missingFilesetIds:
			self.errorMessages.append("%s/%s/%s filesetId is not in LPTS record.\tEROR" % (typeCode, bibleId, filesetId))
		for (typeCode, bibleId, filesetId, stockNo, fieldName) in self.requiredFields:
			self.errorMessages.append("%s/%s/%s LPTS %s field %s is required.\tEROR" % (typeCode, bibleId, filesetId, stockNo, fieldName))
		for (typeCode, bibleId, filesetId, stockNo, fieldName) in self.suggestedFields:
			self.errorMessages.append("%s/%s/%s LPTS %s field %s is missing.\tWARN" % (typeCode, bibleId, filesetId, stockNo, fieldName))
		for (typeCode, bibleId, filesetId, stockNo, fieldName, fieldValue) in self.invalidValues:
			self.errorMessages.append("%s/%s/%s %s has invalid value '%s'.\tEROR" % (typeCode, bibleId, filesetId, fieldName, fieldValue))
		#for (typeCode, bibleId, filesetId, stockNo, status) in self.damIdStatus:
		#	self.errorMessages.append("%s/%s/%s LPTS %s has status = %s.\tWARN" % (typeCode, bibleId, filesetId, stockNo, status))

		if len(self.errorMessages) > 0:
			errorDir = self.config.directory_errors
			pattern = self.config.filename_datetime 
			path = errorDir + "Errors-" + datetime.today().strftime(pattern) + ".out"
			print("openErrorReport", path)
			errorFile = open(path, "w")
			for message in sorted(self.errorMessages):
				(text, level) = message.split("\t", 2)
				formatted = "%s  %s\n" % (level, text)
				errorFile.write(formatted)
				print(formatted, end='')
			errorFile.close()
		print("Num Errors ", len(self.errorMessages))		


if (__name__ == '__main__'):
	args = Validate.parseCommandLine()
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	validate = Validate(args["run"], config, db, lptsReader)
	validate.process()
	validate.reportErrors()


