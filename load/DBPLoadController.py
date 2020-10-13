# DBPLoadController.py

# 1) Run Validate on the files to process
# 2) Move any Fileset that is accepted to uploading
# 3) Perform upload
# 4) Move any fully uploaded fileset to database
# 5) Update fileset related tables
# 6) Move updated fileset to complete


import os
from Config import *
from LPTSExtractReader import *
from Validate import *
from S3Utility import *
from SQLBatchExec import *
from UpdateDBPFilesetTables import *
from UpdateDBPBiblesTable import *
from UpdateDBPLPTSTable import *


class DBPLoadController:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.s3Utility = S3Utility(config)
		self.stockNumRegex = re.compile("__[A-Z0-9]{8}")


	def cleanup(self):
		print("********** CLEANUP **********")
		validateDir = self.config.directory_validate
		for typeCode in os.listdir(validateDir):
			if typeCode == "audio":
				audioDir = validateDir + typeCode
				for bibleId in os.listdir(audioDir):
					bibleDir = audioDir + os.sep + bibleId
					for filesetId in os.listdir(bibleDir):
						filesetDir = bibleDir + os.sep + filesetId
						self._repairFileNames(filesetDir, filesetId)
						self._cleanupHiddenFiles(filesetDir)


	## This corrects filesets that have stock number instead of damId in the filename.
	def _repairFileNames(self, filesetDir, filesetId):
		for fileName in os.listdir(filesetDir):
			if fileName.endswith(".mp3"):
				namePart = fileName.split(".")[0]
				damId = namePart[-10:]
				if self.stockNumRegex.match(damId):
					newFileName = namePart[:-10] + filesetId + ".mp3"
					os.rename(filesetDir + os.sep + fileName, filesetDir + os.sep + newFileName)


	## This method remove hidden files include .* and Thumb.db
	def _cleanupHiddenFiles(self, directory):
		toDelete = []
		self._cleanupHiddenFilesRecurse(toDelete, directory)
		for path in toDelete:
			os.remove(path)


	def _cleanupHiddenFilesRecurse(self, toDelete, directory):
		for pathName in os.listdir(directory):
			fullName = directory + os.sep + pathName
			if pathName.startswith(".") or pathName == "Thumbs.db":
				toDelete.append(fullName)
			if os.path.isdir(fullName):
				self._cleanupHiddenFilesRecurse(toDelete, fullName)


	def validate(self):
		print("********** VALIDATING **********")
		validate = Validate("files", self.config, self.db, self.lptsReader)
		validate.process()
		validate.reportErrors()

		filesets = os.listdir(self.config.directory_accepted)
		for fileset in filesets:
			if fileset.endswith(".csv"):
				filename = fileset.split(".")[0]
				filesetPrefix = filename.replace("_", "/")
				self.s3Utility.promoteFileset(self.config.directory_validate, filesetPrefix)


	def upload(self):
		print("********** UPLOADING TO S3 **********")
		self.s3Utility.uploadAllFilesets()


	def updateDatabase(self):
		print("********** UPDATING DBP **********")
		dbOut = SQLBatchExec(self.config)
		bibles = UpdateDBPBiblesTable(self.config, self.db, dbOut, self.lptsReader)
		bibles.process()
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut)
		filesetList = update.process()
		lptsDBP = UpdateDBPLPTSTable(self.config, self.db, dbOut, self.lptsReader)
		lptsDBP.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute()
		if success:
			for filesetPrefix in filesetList:
				self.s3Utility.promoteFileset(self.config.directory_database, filesetPrefix)


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.cleanup()
	ctrl.validate()
	ctrl.upload()
	ctrl.updateDatabase()
	print("********** COMPLETE **********")



