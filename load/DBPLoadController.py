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
from InputFileset import *
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


	## This corrects filesets that have stock number instead of damId in the filename.
	def repairAudioFileNames(self, inputFilesets):
		for inp in inputFilesets:
			for index in range(len(inp.files)):
				file = inp.files[index]
				if file.name.endswith(".mp3"):
					namePart = file.name.split(".")[0]
					damId = namePart[-10:]
					if self.stockNumRegex.match(damId):
						inp.files[index].name = namePart[:-10] + inp.filesetId[:10] + ".mp3"


	def validate(self, inputFilesets):
		RunStatus.setStatus(RunStatus.VALIDATE)
		validate = Validate(self.config, self.db)
		validate.process(inputFilesets)
		Log.writeLog(self.config)
		for inp in inputFilesets:
			if os.path.isfile(inp.csvFilename):
				InputFileset.upload.append(inp)


	def updateBibles(self):
		dbOut = SQLBatchExec(self.config)
		bibles = UpdateDBPBiblesTable(self.config, self.db, dbOut, self.lptsReader)
		bibles.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("bibles")
		return success		


	def upload(self, inputFilesets):
		RunStatus.setStatus(RunStatus.UPLOAD)
		self.s3Utility.uploadAllFilesets(inputFilesets)


	def updateFilesetTables(self, inputFilesets):
		RunStatus.setStatus(RunStatus.UPDATE)
		inp = inputFilesets
		dbOut = SQLBatchExec(self.config)
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut)
		video = UpdateDBPVideoTables(self.config, self.db, dbOut)
		for inp in inputFilesets:
			hashId = update.processFileset(inp.typeCode, inp.bibleId, inp.filesetId, inp.fullPath(), inp.csvFilename, inp.databasePath)
			if inp.typeCode == "video":
				video.processFileset(inp.filesetPrefix, inp.filenames(), hashId)
			dbOut.displayCounts()
			success = dbOut.execute(inp.filesetId)
			if success:
				InputFileset.complete.append(inp)
			else:
				print("********** Fileset Table %s Update Failed **********" % (inp.filesetId))


	def updateLPTSTables(self):
		dbOut = SQLBatchExec(self.config)
		lptsDBP = UpdateDBPLPTSTable(self.config, dbOut, self.lptsReader)
		lptsDBP.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("lpts")
		return success


if (__name__ == '__main__'):
	config = Config.shared()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	InputFileset.validate = InputFileset.filesetCommandLineParser(config, lptsReader)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.repairAudioFileNames(InputFileset.validate)
	ctrl.validate(InputFileset.validate)
	if ctrl.updateBibles():
		ctrl.upload(InputFileset.upload)
		ctrl.updateFilesetTables(InputFileset.database)
		if ctrl.updateLPTSTables():
			if Log.totalErrorCount() == 0:
				RunStatus.setStatus(RunStatus.SUCCESS)
			else:
				RunStatus.setStatus(RunStatus.FAILURE)
		else:
			RunStatus.setStatus(RunStatus.FAILURE)
			print("********** LPTS Tables Update Failed **********")
	else:
		RunStatus.setStatus(RunStatus.FAILURE)
		print("********** Bibles Table Update Failed **********")
	for inputFileset in InputFileset.complete:
		print("Completed: ", inputFileset.filesetId)

# Prepare by getting some local data into a test bucket
# aws s3 --profile Gary sync /Volumes/FCBH/all-dbp-etl-test/ENGESVN2DA s3://test-dbp-etl/ENGESVN2DA
# aws s3 --profile Gary sync /Volumes/FCBH/all-dbp-etl-test/ENGESVN2DA16 s3://test-dbp-etl/audio/ENGESV/ENGESVN2DA16
# aws s3 --profile Gary sync /Volumes/FCBH/all-dbp-etl-test/HYWWAVN2ET s3://test-dbp-etl/HYWWAVN2ET
# aws s3 --profile Gary sync /Volumes/FCBH/all-dbp-etl-test/ENGESVP2DV s3://test-dbp-etl/ENGESVP2DV

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test ENGESVN2DA
# time python3 load/TestCleanup.py test ENGESVN2DA16
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ ENGESVN2DA ENGESVN2DA16
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ HYWWAVN2ET
# time python3 load/DBPLoadController.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3
# time python3 load/TestCleanup.py test ENGESVN2DA
# time python3 load/TestCleanup.py test ENGESVN2DA16
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test s3://test-dbp-etl ENGESVN2DA
# time python3 load/DBPLoadController.py test s3://test-dbp-etl text/ENGESV/ENGESVN2DA16
# time python3 load/DBPLoadController.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/DBPLoadController.py test s3://test-dbp-etl ENGESVP2DV

# time python3 load/TestCleanup.py test UNRWFWP1DA
# time python3 load/TestCleanup.py test UNRWFWP1DA16
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ audio/UNRWFW/UNRWFWP1DA16

# Some video uploads
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV
# time python3 load/DBPLoadController.py test-video /Volumes/FCBH/all-dbp-etl-test/ video/ENGESV/ENGESVP2DV
# time python3 load/DBPLoadController.py test-video /Volumes/FCBH/all-dbp-etl-test/ video/ENGESX/ENGESVP2DV

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test GNWNTM
# time python3 load/DBPLoadController.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ GNWNTMN2ET
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ text/GNWNTM/GNWNTMN2ET




