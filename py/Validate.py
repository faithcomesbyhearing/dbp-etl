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
		print(len(self.bibleIdMap.keys()), " BibleId Nums found")
		#for (bibleId, recs) in bibleIdMap.items():
		#	if len(recs) > 1:
		#		print(bibleId)
		#		for rec in recs:
		#			print("\t", rec.Reg_StockNumber())
		self.bibleId2Map = self.lpts.getBibleId2Map()
		for (bibleId, recs) in self.bibleId2Map.items():
			print(bibleId)
			for rec in recs:
				print("\t", "bibleId:", rec.DBP_Equivalent(), " stockno:", rec.Reg_StockNumber())
				print("\t\t", "3 Orthographys:", rec.x0031_Orthography(), ";", rec.x0032_Orthography(), ";", rec.x0033_Orthography(), ";")
				filesetIds = rec.DamIds()
				for filesetId in filesetIds.keys():
					print("\t\t", filesetId)
		sys.exit()
		self.filesetIdMap = self.lpts.getFilesetIdMap()
		#for filesetId, records in self.filesetIdMap.items():
		#	if len(records) > 1:
		#		print(filesetId)
		#		for rec in records:
		#			print("\t", rec.DBP_Equivalent(), rec.Reg_StockNumber())
		#sys.exit()


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

		## create bibles map bibleId: [filesetid]
		bibles = {}
		for filePrefix in filesets.keys():
			(typeCode, bibleId, filesetId) = filePrefix.split("/")
			filesetList = bibles.get(bibleId, [])
			filesetList.append(filesetId)
			bibles[bibleId] = filesetList

		for bibleId, filesets in bibles.items():
			print(bibleId, filesets)

		## validate the LPTS data
		## filenameParser
		## prepare errors
		## report on what is accept, what is quarantine and what is duplicated


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



	def validateLPTSExtract(self, directory):
		required = {"Copyrightc", "Copyrightp", "Copyright_Video", "DBP_Equivalent", 
				"ISO", "LangName", "Licensor", "Reg_StockNumber", "Volumne_Name"}
		possible = {"_x0031_Orthography"}
		## build map of extract by DBP_Equivalent, include DBP_Equivalent2, but report as warning
		## check DBP2 for uniqueness also
		## look for duplicate bible_id's
		## look for duplicate fileset_id's
		## look for duplicate stock nos
		## 
		## create error messages for all duplicates
		## for each bible_id, lookup in extract.  i.e. extract must have a map by DBP_Equivanent
		## have set of names required, and set of names not-required, but expected
		## check that all required fields are present, and prepare error messages, but continue
		## check that all non-required fields are present, and prepare error messages, and continue
		## Use damid fieldname to lookup filesets, and prepare an error for any missing.
		## Possibly prepare a warning for any that are NOT included






args = Validate.parseCommandLine()
print(args)

validate = Validate(args)
validate.process()

#config = Config(profile)
#validate = Validate(config, args)
#validate.process()

"""
DBP_Equivalent - required  count 3805
DBP_Equivalent2 - what is this count 16
ISO - required - count 3947
LangName - required - count 3953
_x0031_Orthography - not required - count 1568
_x0032 - count 128
x_0033 - count 42
Volumne_Name - required I think - count 3468
Copyrightc - required for what? - count 4226
Copyrightp - required for what? - count 3983
Copyright_Video - required for video - count 3953
Licensor - required ? count 2804
Reg_StockNumber - required - count 3953



Compare these to damId’s being loaded
Reg_NTAudioDamID1" validate="tbd" clean="nullify1"/>
Reg_NTAudioDamID2" validate="tbd" clean="nullify1"/>
ND_NTAudioDamID1" validate="tbd" clean="nullify1"/>
ND_NTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTAudioDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTAudioDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="CAudioDAMID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="CAudioDamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTAudioDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_NTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_NTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="Reg_OTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID1" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID2" validate="tbd" clean="nullify1"/>
			<lptsXML name="ND_OTTextDamID3" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Matthew_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Mark_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_Luke_DamStockNo" validate="tbd" clean="nullify1"/>
			<lptsXML name="Video_John_DamStockNo" validate="tbd" clean="nullify1"/>
"""