# Validate.py

import os
import sys
import zipfile
from datetime import datetime
from Config import *
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
			

	def __init__(self, args):
		self.config = Config(args["config"])
		self.runType = args["run"]
		lpts = LPTSExtractReader(self.config)
		self.bibleIdMap = lpts.getBibleIdMap()
		print(len(self.bibleIdMap.keys()), " BibleIds found.")
		self.errorMessages = []
		self.invalidFileExt = []
		self.missingBibleIds = []
		self.missingFilesetIds = []
		self.requiredFields = []
		self.suggestedFields = []
		self.damIdStatus = []


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
		parser.process3(filesets, self.bibleIdMap, self.errorMessages)
		parser.summary3()

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
						if file.endswith("csv"):
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
			lptsRecords = self.bibleIdMap.get(bibleId)
			if lptsRecords == None:
				self.missingBibleIds.append((typeCode, bibleId))
			else:

				## Validate filesetIds against LPTS
				filesetIds = inputIdMap[biblePrefix]
				lptsFilesetIdSet = self.getFilesetIdSet(typeCode, bibleId, lptsRecords)
				self.validateFilesetIds(typeCode, bibleId, filesetIds, lptsFilesetIdSet)

				## Validate Required fields are present
				for (index, record) in lptsRecords:
					stockNo = record.Reg_StockNumber()
					if record.Copyrightc() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Copyrightc"))
					if record.Copyrightp() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Copyrightp"))
					if record.Copyright_Video() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Copyright_Video"))
					if record.ISO() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "ISO"))
					if record.LangName() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "LangName"))
					if record.Licensor() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Licensor"))
					if record.Reg_StockNumber() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Reg_StockNumber"))
					if record.Volumne_Name() == None:
						self.requiredFields.append((typeCode, bibleId, stockNo, "Volumne_Name"))

					if typeCode == "text":
						if record.Orthography(index) == None:
							fieldName = "_x003%d_Orthography" % (index)
							self.requiredFields.append((typeCode, bibleId, stockNo, fieldName))
					elif typeCode == "audio":
						if record.Orthography(index) == None:
							fieldName = "_x003%d_Orthography" % (index)							
							self.suggestedFields.append((typeCode, bibleId, stockNo, fieldName))


	def getFilesetIdSet(self, typeCode, bibleId, lptsRecordList):
		if len(lptsRecordList) == 0:
			return set()
		elif len(lptsRecordList) == 1:
			(index, record) = lptsRecordList[0]
			damIdMap = record.DamIds(typeCode, index)
			self.validateStatus(typeCode, bibleId, damIdMap, record)
			return set(damIdMap.keys())
		else:
			result = set()
			for (index, record) in lptsRecordList:
				damIdMap = record.DamIds(typeCode, index)
				self.validateStatus(typeCode, bibleId, damIdMap, record)
				result = result.union(set(damIdMap.keys()))
			return result


	def validateStatus(self, typeCode, bibleId, damIdMap, record):
		for (filesetId, statuses) in damIdMap.items():
			for (statusKey, status) in statuses:
				if not status in {"Live", "live"}:
					self.damIdStatus.append((typeCode, bibleId, filesetId, record.Reg_StockNumber(), statusKey, status))


	def validateFilesetIds(self, typeCode, bibleId, inputFilesetIdList, lptsFilesetIdSet):
		if typeCode == "audio":
			inputFilesetIds = []
			for filesetId in inputFilesetIdList:
				inputFilesetIds.append(filesetId[:10]) # reduce input filesetId to 10 char
			lptsFilesetIds = lptsFilesetIdSet

		elif typeCode == "text":
			inputFilesetIds = inputFilesetIdList
			lptsFilesetIds = set()
			for filesetId in lptsFilesetIdSet:
				lptsFilesetIds.add(filesetId[:6]) # reduce lpts filesetId to 6 char
		else:
			inputFilesetIds = inputFilesetIdList
			lptsFilesetIds = lptsFilesetIdSet

		for filesetId in inputFilesetIds:
			if not filesetId in lptsFilesetIds:
				self.missingFilesetIds.append((typeCode, bibleId, filesetId))


	def reportErrors(self):
		for (typeCode, bibleId, filesetId, filename) in self.invalidFileExt:
			self.errorMessages.append("%s/%s/%s/%s has an invalid file ext." % (typeCode, bibleId, filesetId, filename))
		for (typeCode, bibleId) in self.missingBibleIds:
			self.errorMessages.append("%s/%s bibleId is not in LPTS." % (typeCode, bibleId,))
		for (typeCode, bibleId, filesetId) in self.missingFilesetIds:
			self.errorMessages.append("%s/%s/%s filesetId is not in LPTS record." % (typeCode, bibleId, filesetId))
		for (typeCode, bibleId, stockNo, fieldName) in self.requiredFields:
			self.errorMessages.append("%s/%s LPTS %s field %s is required." % (typeCode, bibleId, stockNo, fieldName))
		for (typeCode, bibleId, stockNo, fieldName) in self.suggestedFields:
			self.errorMessages.append("%s/%s LPTS %s field %s is missing." % (typeCode, bibleId, stockNo, fieldName))
		for (typeCode, bibleId, filesetId, stockNo, statusName, status) in self.damIdStatus:
			self.errorMessages.append("%s/%s/%s LPTS %s has %s = %s." % (typeCode, bibleId, filesetId, stockNo, statusName, status))
		for message in sorted(self.errorMessages):
			print(message)	

		errorDir = self.config.directory_errors
		pattern = self.config.filename_datetime 
		path = errorDir + "Errors-" + datetime.today().strftime(pattern) + ".out"
		print("openErrorReport", path)
		errorFile = open(path, "w")
		for message in sorted(self.errorMessages):
			errorFile.write(message + "\n")
		errorFile.close()		


args = Validate.parseCommandLine()
validate = Validate(args)
validate.process()
validate.reportErrors()


