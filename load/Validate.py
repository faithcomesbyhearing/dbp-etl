# Validate.py

import os
import sys
import zipfile
from datetime import datetime
from Config import *
from Log import *
from PreValidate import *
from SQLUtility import *
from InputReader import *
from LPTSExtractReader import *
from FilenameParser import *
from FilenameReducer import *
from UpdateDBPTextFilesets import *


class Validate:	

	def __init__(self, runType, config, db, lptsReader):
		self.config = config
		self.db = db
		self.runType = runType
		self.lptsReader = lptsReader
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
			filesets = reader.fileListing(self.config.directory_upload)
			filesets = reader.fileListing(self.config.directory_database)

		else:
			print("ERROR: run_type must be files or bucketlists.")
			sys.exit()

		## Validate filetypes
		for (filePrefix, filenames) in filesets.items():
			logger = Log.getLogger(filePrefix)
			(typeCode, bibleId, filesetId) = filePrefix.split("/")
			for (filename, filesize, lastmodified) in filenames:
				ext = os.path.splitext(filename)[1]
				if typeCode == "audio" and ext not in {".mp3", ".opus", ".webm", ".m4a"}:
					logger.invalidFileExt(filename)
				elif typeCode == "text" and not ext in {".html", ".usx"}:
					logger.invalidFileExt(filename)
				elif typeCode == "video" and ext != ".mp4":
					logger.invalidFileExt(filename)

		## Validate Text Filesets
		texts = UpdateDBPTextFilesets(self.config, self.db, None, self.lptsReader)
		s3 = S3Utility(self.config)
		for (filePrefix, filenames) in filesets.items():
			if filePrefix.startswith("text"):
				(typeCode, bibleId, filesetId) = filePrefix.split("/")
				errorTuple = texts.validateFileset(bibleId, filesetId)
				if errorTuple != None:
					logger = Log.getLogger(filePrefix)
					logger.messageTuple(errorTuple)		

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
		parser.process3(filesets, self.lptsReader)

		find = FindDuplicateFilesets(self.config)
		duplicates = find.findDuplicates()
		find.moveDuplicates(duplicates)


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
						if (file.endswith(".csv") or file.endswith(".db")) and not file.startswith("._"):
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
		validate = PreValidate(self.lptsReader)
		for biblePrefix in sorted(inputIdMap.keys()):
			(typeCode, bibleId) = biblePrefix.split("/")
			filesetIds = inputIdMap[biblePrefix]
			for filesetId in filesetIds:
				logger = Log.getLogger3(typeCode, bibleId, filesetId)
				validate.validateLPTS(typeCode, bibleId, filesetId)
				logger.addPreValidationErrors(validate.messages)

				## These validations require MySQL and so are done here, not PreValidate
				if typeCode == "text":
					(lptsRecord, index) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
					if lptsRecord != None:

						scriptName = lptsRecord.Orthography(index)
						if scriptName != None and scriptName not in self.scriptNameSet:
							logger.invalidValues(stockNo, "_x003n_Orthography", scriptName)

						numeralsName = lptsRecord.Numerals()
						if numeralsName != None and numeralsName not in self.numeralsSet:
							logger.invalidValues(stockNo, "Numerals", numeralsName)


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	validate = Validate("files", config, db, lptsReader)
	validate.process()
	validate.reportErrors()


