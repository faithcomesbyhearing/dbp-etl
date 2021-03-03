# DBPLoadController.py

# 1) Run Validate on the files to process
# 2) Move any Fileset that is accepted to uploading
# 3) Perform upload
# 4) Move any fully uploaded fileset to database
# 5) Update fileset related tables
# 6) Move updated fileset to complete


import os
from Config import *
from RunStatus import *
from LPTSExtractReader import *
from Log import *
from PreValidate import *
from Validate import *
from S3Utility import *
from SQLBatchExec import *
from UpdateDBPFilesetTables import *
from UpdateDBPBiblesTable import *
from UpdateDBPLPTSTable import *
from UpdateDBPVideoTables import *


class DBPLoadController:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.s3Utility = S3Utility(config)
		self.stockNumRegex = re.compile("__[A-Z0-9]{8}")


	def preprocessUploadAWS(self):
		RunStatus.setStatus(RunStatus.PREPROCESS)
		validate = PreValidate(self.lptsReader)
		for filesetId in [f for f in os.listdir(self.config.directory_upload_aws) if not f.startswith('.')]:
			logger = Log.getLogger(filesetId)
			results = validate.validateFilesetId(filesetId)
			logger.addPreValidationErrors(validate.messages)
			if results != None and Log.totalErrorCount() == 0:
				(typeCode, bibleId) = results
				directory = self.config.directory_upload + typeCode + os.sep
				if not os.path.isdir(directory):
					os.mkdir(directory)
				directory += bibleId + os.sep
				if not os.path.isdir(directory):
					os.mkdir(directory)
				dbpFilesetId = filesetId[:6] if typeCode == "text" else filesetId
				sourceDir = self.config.directory_upload_aws + filesetId + os.sep
				targetDir = directory + dbpFilesetId + os.sep
				os.replace(sourceDir, targetDir)

		if Log.totalErrorCount() > 0:
			Log.writeLog(self.config)
			sys.exit()


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
		RunStatus.setStatus(RunStatus.VALIDATE)
		print("********** VALIDATING **********", flush=True)
		validate = Validate("files", self.config, self.db, self.lptsReader)
		validate.process()
		Log.writeLog(self.config)


	def upload(self):
		RunStatus.setStatus(RunStatus.UPLOAD)
		print("********** UPLOADING TO S3 **********", flush=True)
		filesets = self._acceptedFilesets(self.config.directory_upload)
		self.s3Utility.uploadAllFilesets(filesets)


	def updateFilesetTables(self):
		RunStatus.setStatus(RunStatus.UPDATE)
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
	config = Config.shared()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.preprocessUploadAWS()
	ctrl.cleanup()
	ctrl.validate()
	if ctrl.updateBibles():
		ctrl.upload()
		ctrl.updateFilesetTables()
		if ctrl.updateLPTSTables():
			RunStatus.setStatus(RunStatus.SUCCESS)
			print("********** COMPLETE **********")
		else:
			RunStatus.setStatus(RunStatus.FAILURE)
			print("********** LPTS Tables Update Failed **********")
	else:
		RunStatus.setStatus(RunStatus.FAILURE)
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
