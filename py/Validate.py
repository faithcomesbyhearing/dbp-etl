# Validate.py

import os
import sys
import zipfile
from datetime import datetime
from Config import *
from InputReader import *
from LPTSExtractReader import *


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
		self.lpts = LPTSExtractReader(self.config)
		#self.lpts.checkUniqueNames()
		self.bibleIdMap = self.lpts.getBibleIdMap()
		print(len(self.bibleIdMap.keys()), " BibleIds found.")
		#for (bibleId, recs) in bibleIdMap.items():
		#	if len(recs) > 1:
		#		print(bibleId)
		#		for rec in recs:
		#			print("\t", rec.Reg_StockNumber())
		self.bibleId2Map = self.lpts.getBibleId2Map()
		print(len(self.bibleId2Map.keys()), " BibleId2s found.")
		#for (bibleId, recs) in self.bibleId2Map.items():
		#	print(bibleId)
		#	for rec in recs:
		#		print("\t", "bibleId:", rec.DBP_Equivalent(), " stockno:", rec.Reg_StockNumber())
		#		print("\t\t", "3 Orthographys:", rec.x0031_Orthography(), ";", rec.x0032_Orthography(), ";", rec.x0033_Orthography(), ";")
		#		filesetIds = rec.DamIds()
		#		for filesetId in filesetIds.keys():
		#			print("\t\t", filesetId)
		#sys.exit()
		self.filesetIdMap = self.lpts.getFilesetIdMap()
		print(len(self.filesetIdMap.keys()), " FilesetIds found.")
		#for filesetId, records in self.filesetIdMap.items():
		#	if len(records) > 1:
		#		print(filesetId)
		#		for rec in records:
		#			print("\t", rec.DBP_Equivalent(), rec.Reg_StockNumber())
		#sys.exit()
		self.missingBibleIds = []
		self.missingFilesetIds = []
		self.requiredFields = []
		self.suggestedFields = []


	def process(self):
		self.prepareDirectory(self.config.directory_validate)
		self.prepareDirectory(self.config.directory_accepted)
		self.prepareDirectory(self.config.directory_quarantine)
		self.prepareDirectory(self.config.directory_duplicate)
		self.prepareDirectory(self.config.directory_errors)

		## get filesets: a map typeCode/bibleId/filesetId: [(filename, fileSize, datetime)]
		reader = InputReader(self.config)
		if self.runType == "bucketlists":
#			filesets = reader.bucketListing(self.config.s3_bucket)
			filesets = reader.bucketListing(self.config.s3_vid_bucket)

		elif self.runType == "files":
			print("do it")
			## In reader move the core section into a common routine
			## that processes filenames
			## In reader add new method to read directory of files
			## read a directory listing into the identical form

		else:
			print("ERROR: run_type must be files or bucketlists.")
			sys.exit()

		## Reduce input files to bibleId: [filesetId]
		inputIdMap = {}
		for filePrefix in filesets.keys():
			(typeCode, bibleId, filesetId) = filePrefix.split("/")
			filesetIdList = inputIdMap.get(bibleId, [])
			filesetIdList.append(filesetId)
			inputIdMap[bibleId] = filesetIdList

		self.validateLPTS(inputIdMap)


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
		for bibleId in sorted(inputIdMap.keys()):

			## Validate bibleId agains LPTS
			lptsRecords = self.bibleIdMap.get(bibleId)
			if lptsRecords == None:
				self.missingBibleIds.append(bibleId)
			else:

				## Validate filesetIds against LPTS
				lptsFilesetIdSet = self.getFilesetIdSet(lptsRecords)
				filesetIds = inputIdMap[bibleId]
				for filesetId in filesetIds:
					if not filesetId in lptsFilesetIdSet:
						self.missingFilesetIds.append((filesetId, bibleId))

				## Validate Required fields are present
				for record in lptsRecords:
					if record.Copyrightc() == None:
						self.requiredFields.append(("Copyrightc", bibleId))
					if record.Copyrightp() == None:
						self.requiredFields.append(("Copyrightp", bibleId))
					if record.Copyright_Video() == None:
						self.requiredFields.append(("Copyright_Video", bibleId))
					if record.ISO() == None:
						self.requiredFields.append(("ISO", bibleId))
					if record.LangName() == None:
						self.requiredFields.append(("LangName", bibleId))
					if record.Licensor() == None:
						self.requiredFields.append(("Licensor", bibleId))
					if record.Reg_StockNumber() == None:
						self.requiredFields.append(("Reg_StockNumber", bibleId))
					if record.Volumne_Name() == None:
						self.requiredFields.append(("Volumne_Name", bibleId))

					if record.x0031_Orthography() == None:
						self.suggestedFields.append(("_x0031_Orthography", bibleId))


	def getFilesetIdSet(self, lptsRecordList):
		if len(lptsRecordList) == 0:
			return set()
		elif len(lptsRecordList) == 1:
			return set(lptsRecordList[0].DamIds().keys())
		else:
			result = set()
			for record in lptsRecordList:
				result.union(set(record.DamIds().keys()))
			return result


	def reportErrors(self):
		for bibleId in self.missingBibleIds:
			print("%s bibleId is not in LPTS." % (bibleId,))
		for (filesetId, bibleId) in self.missingFilesetIds:
			print("%s filesetId is not in LPTS record of %s" % (filesetId, bibleId))
		for (fieldName, bibleId) in self.requiredFields:
			print("%s field is not in LPTS record of %s" % (fieldName, bibleId))
		for (fieldName, bibleId) in self.suggestedFields:
			print("%s field is suggested for LPTS record of %s" % (fieldName, bibleId))		




args = Validate.parseCommandLine()
validate = Validate(args)
validate.process()
validate.reportErrors()


