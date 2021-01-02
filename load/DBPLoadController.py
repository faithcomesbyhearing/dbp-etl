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
from Log import *
from Validate import *
from S3Utility import *
from SQLBatchExec import *
from UpdateDBPFilesetTables import *
from UpdateDBPBiblesTable import *
from UpdateDBPLPTSTable import *
from TranscodeVideo import *
from UpdateDBPVideoTables import *


class DBPLoadController:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.s3Utility = S3Utility(config)
		self.stockNumRegex = re.compile("__[A-Z0-9]{8}")


	def cleanup(self):
		print("********** CLEANUP **********", flush=True)
		validateDir = self.config.directory_upload
		for typeCode in os.listdir(validateDir):
			if typeCode in {"audio", "video", "text"}:
				audioDir = validateDir + typeCode
				for bibleId in os.listdir(audioDir):
					bibleDir = audioDir + os.sep + bibleId
					for filesetId in os.listdir(bibleDir):
						filesetDir = bibleDir + os.sep + filesetId
						self._cleanupHiddenFiles(filesetDir)
						if typeCode == "audio":
							self._repairFileNames(filesetDir, filesetId)



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
		if os.path.isdir(directory):
			for pathName in os.listdir(directory):
				fullName = directory + os.sep + pathName
				if pathName.startswith(".") or pathName == "Thumbs.db":
					toDelete.append(fullName)
				if os.path.isdir(fullName):
					self._cleanupHiddenFilesRecurse(toDelete, fullName)


	def updateBibles(self):
		print("********** UPDATING Bibles Table **********", flush=True)
		dbOut = SQLBatchExec(self.config)
		bibles = UpdateDBPBiblesTable(self.config, self.db, dbOut, self.lptsReader)
		bibles.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("bibles")
		return success		


	def validate(self):
		print("********** VALIDATING **********", flush=True)
		validate = Validate("files", self.config, self.db, self.lptsReader)
		validate.process()
		Log.writeLog(self.config)


	def upload(self):
		print("********** UPLOADING TO S3 **********", flush=True)
		filesets = self._acceptedFilesets(self.config.directory_upload)
		self.s3Utility.uploadAllFilesets(filesets)
		TranscodeVideo.transcodeVideoFilesets(self.config, filesets)


	def updateFilesetTables(self):
		print("********** UPDATING Fileset Tables **********", flush=True)
		dbOut = SQLBatchExec(self.config)
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut)
		video = UpdateDBPVideoTables(self.config, self.db, dbOut)
		filesets = self._acceptedFilesets(self.config.directory_database)
		for (typeCode, bibleId, filesetId, filesetPrefix, csvFilename) in filesets:
			hashId = update.processFileset(typeCode, bibleId, filesetId, csvFilename)
			if typeCode == "video":
				video.processFileset(filesetPrefix, hashId)
			dbOut.displayCounts()
			success = dbOut.execute(filesetId)
			if success:
				self.s3Utility.promoteFileset(self.config.directory_database, filesetPrefix)
			else:
				print("********** Fileset Table %s Update Failed **********" % (filesetId))


	def updateLPTSTables(self):
		print("********** UPDATING LPTS Tables **********", flush=True)
		dbOut = SQLBatchExec(self.config)
		lptsDBP = UpdateDBPLPTSTable(self.config, dbOut, self.lptsReader)
		lptsDBP.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("lpts")
		return success


	def _acceptedFilesets(self, directory):
		results = []
		for typeCode in [f for f in os.listdir(directory) if not f.startswith('.')]:
			for bibleId in [f for f in os.listdir(directory + typeCode) if not f.startswith('.')]:
				for filesetId in [f for f in os.listdir(directory + typeCode + os.sep + bibleId) if not f.startswith('.')]:
					filesetPrefix = typeCode + "/" + bibleId + "/" + filesetId + "/"
					csvFilename = "%s%s_%s_%s.csv" % (self.config.directory_accepted, typeCode, bibleId, filesetId)
					if os.path.exists(csvFilename):
						results.append((typeCode, bibleId, filesetId, filesetPrefix, csvFilename))
		return results


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.cleanup()
	ctrl.validate()
	if ctrl.updateBibles():
		ctrl.upload()
		ctrl.updateFilesetTables()
		if ctrl.updateLPTSTables():
			print("********** COMPLETE **********")
		else:
			print("********** LPTS Tables Update Failed **********")
	else:
		print("********** Bibles Table Update Failed **********")

"""
config = Config()
db = SQLUtility(config)
lptsReader = LPTSExtractReader(config)
ctrl = DBPLoadController(config, db, lptsReader)
filesets = ctrl._acceptedFilesets(config.directory_complete)
for (typeCode, bibleId, filesetId, filesetPrefix, csvFilename) in filesets:
	print(typeCode, bibleId, filesetId, filesetPrefix, csvFilename)
"""
