# DBPLoadController.py

# 1) Run Validate on the files to process
# 2) Move any Fileset that is accepted to uploading
# 3) Perform upload
# 4) Move any fully uploaded fileset to database
# 5) Update fileset related tables
# 6) Move updated fileset to complete


import os
from Config import *
from LanguageReader import *
from Log import *
from InputFileset import *
from InputProcessor import *
from Validate import *
from S3Utility import *
from SQLBatchExec import *
from UpdateDBPFilesetTables import *
from UpdateDBPBiblesTable import *
from UpdateDBPLPTSTable import *
from UpdateDBPVideoTables import *
from UpdateDBPBibleFilesSecondary import *


class DBPLoadController:

	def __init__(self, config, db, languageReader):
		self.config = config
		self.db = db
		self.languageReader = languageReader
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
		validate = Validate(self.config, self.db)
		validate.process(inputFilesets)
		for inp in inputFilesets:
			if os.path.isfile(inp.csvFilename):
				InputFileset.upload.append(inp)
			else:
				RunStatus.set(inp.filesetId, False)


	def updateBibles(self):
		dbOut = SQLBatchExec(self.config)
		bibles = UpdateDBPBiblesTable(self.config, self.db, dbOut, self.languageReader)
		bibles.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("bibles")
		RunStatus.set(RunStatus.BIBLE, success)
		return success		


	def upload(self, inputFilesets):
		self.s3Utility.uploadAllFilesets(inputFilesets)
		secondary = UpdateDBPBibleFilesSecondary(self.config, None, None)
		secondary.createAllZipFiles(inputFilesets)
		Log.writeLog(self.config)


	def updateFilesetTables(self, inputFilesets):
		inp = inputFilesets
		dbOut = SQLBatchExec(self.config)
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut)
		video = UpdateDBPVideoTables(self.config, self.db, dbOut)
		for inp in inputFilesets:
			hashId = update.processFileset(inp)
			if inp.typeCode == "video":
				video.processFileset(inp.filesetPrefix, inp.filenames(), hashId)
			dbOut.displayCounts()
			success = dbOut.execute(inp.batchName())
			RunStatus.set(inp.filesetId, success)
			if success:
				InputFileset.complete.append(inp)
			else:
				print("********** Fileset Table %s Update Failed **********" % (inp.filesetId))


	def updateLPTSTables(self):
		dbOut = SQLBatchExec(self.config)
		lptsDBP = UpdateDBPLPTSTable(self.config, dbOut, self.languageReader)
		lptsDBP.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("lpts")
		RunStatus.set(RunStatus.LPTS, success)
		return success


if (__name__ == '__main__'):
	from LanguageReaderCreator import *
	config = Config()
	AWSSession.shared() # ensure AWSSession init
	db = SQLUtility(config)
	languageReader = LanguageReaderCreator().create(config)
	ctrl = DBPLoadController(config, db, languageReader)
	if len(sys.argv) != 2:
		InputFileset.validate = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
		ctrl.repairAudioFileNames(InputFileset.validate)
		ctrl.validate(InputFileset.validate)
		if ctrl.updateBibles():
			ctrl.upload(InputFileset.upload)
			ctrl.updateFilesetTables(InputFileset.database)
			ctrl.updateLPTSTables()
		for inputFileset in InputFileset.complete:
			print("Completed: ", inputFileset.filesetId)
	else:
		ctrl.updateBibles()
		ctrl.updateLPTSTables()
	RunStatus.exit()

# Get current lpts-dbp.xml
# aws --profile DBP_DEV s3 cp s3://dbp-etl-upload-newdata-fiu49s0cnup1yr0q/lpts-dbp.xml /Volumes/FCBH/bucket_data/lpts-dbp.xml

#python3 load/DBPLoadController.py test s3://etl-development-input Spanish_N2SPNTLA_USX #works with refactor

# python3 load/DBPLoadController.py test s3://etl-development-input "French_N1 & O1 FRABIB_USX"
# python3 load/TestCleanup.py test FRABIBN_ET-usx
# python3 load/TestCleanup.py test FRABIBO_ET-usx





# Clean up filesets in dbp-staging and dbp-vid-staging

# Prepare by getting some local data into a test bucket
# aws s3 --profile dbp-etl-dev sync --acl bucket-owner-full-control /Volumes/FCBH/all-dbp-etl-test/audio/UNRWFW/UNRWFWP1DA s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/UNRWFWP1DA
# aws s3 --profile dbp-etl-dev sync --acl bucket-owner-full-control /Volumes/FCBH/all-dbp-etl-test/HYWWAVN2ET s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/HYWWAVN2ET
# aws s3 --profile dbp-etl-dev sync --acl bucket-owner-full-control /Volumes/FCBH/all-dbp-etl-test/ENGESVP2DV s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/ENGESVP2DV

# No parameter, should execute only bible and lpts updates
# time python3 load/DBPLoadController.py test

# Successful tests with source on local drive
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test HYWWAVN_ET-usx
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ HYWWAVN2ET
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3
# time python3 load/TestCleanup.py test UNRWFWP1DA
# time python3 load/TestCleanup.py test UNRWFWP1DA-opus16
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr UNRWFWP1DA
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test HYWWAVN_ET-usx
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr HYWWAVN2ET
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr ENGESVP2DV

# Combined test of two dissimilar filesets on s3
# time python3 load/TestCleanup.py test UNRWFWP1DA
# time python3 load/TestCleanup.py test HYWWAV
# time python3 load/TestCleanup.py test HYWWAVN_ET-usx
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr UNRWFWP1DA HYWWAVN2ET

# Some video uploads
# time python3 load/TestCleanup.py test ENGESVP2DV
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ video/ENGESV/ENGESVP2DV
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ video/ENGESX/ENGESVP2DV

# Successful tests with source on local drive and full path
# time python3 load/TestCleanup.py test GNWNTM
# time python3 load/TestCleanup.py test GNWNTMN_ET-usx
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ GNWNTMN2ET

# time python3 load/TestCleanup.py test GNWNTM
# time python3 load/TestCleanup.py test GNWNTMN_ET-usx
# time python3 load/DBPLoadController.py test /Volumes/FCBH/all-dbp-etl-test/ text/GNWNTM/GNWNTMN2ET

### prepare test data in bucket
### aws --profile DBP_DEV s3 sync /Volumes/FCBH/TextStockNo/Barai_N2BBBWBT_USX/ s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/Barai_N2BBBWBT_USX/
### aws --profile DBP_DEV s3 sync /Volumes/FCBH/TextStockNo/Orma_N2ORCBTL_USX/ s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/Orma_N2ORCBTL_USX/
### aws --profile DBP_DEV s3 sync /Volumes/FCBH/TextStockNo/Urdu_N2URDPAK_USX/ s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/Urdu_N2URDPAK_USX/

# Test stock number upload from Drive with path
# time python3 load/TestCleanup.py test BBBWBT
# time python3 load/TestCleanup.py test BBBWBTN_ET-usx
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/ Barai_N2BBBWBT_USX

# time python3 load/TestCleanup.py test ORCBTL
# time python3 load/TestCleanup.py test ORCBTLN_ET-usx
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/ Orma_N2ORCBTL_USX

# time python3 load/TestCleanup.py test URDPAK
# time python3 load/TestCleanup.py test URDPAKN_ET-usx
# time python3 load/DBPLoadController.py test s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr/ Urdu_N2URDPAK_USX

# python3 load/TestCleanup.py test ABIWBT
# python3 load/TestCleanup.py test ABIWBTN_ET-usx
# python3 load/DBPLoadController.py test s3://dbp-etl-mass-batch "Abidji N2ABIWBT/05 DBP & GBA/Abidji_N2ABIWBT/Abidji_N2ABIWBT_USX"

# This one BiblePublisher has two copies of 1CO:16, but I can only find one in the USX file.
# python3 load/TestCleanup.py test ACHBSU
# python3 load/TestCleanup.py test ACHBSUN_ET-usx
# python3 load/DBPLoadController.py test s3://dbp-etl-mass-batch "Acholi N2ACHBSU/05 DBP & GBA/Acholi_N2ACHBSU - Update/Acholi_N2ACHBSU_USX"

# python3 load/TestCleanup.py test CRXWYI
# python3 load/TestCleanup.py test CRXWYIP_ET-usx
# python3 load/TestCleanup.py test CRXWYIN_ET-usx
# python3 load/DBPLoadController.py test s3://dbp-etl-mass-batch "Carrier, Central N2CRXWYI/05 DBP & GBA/Carrier, Central_P1CRXWYI/Carrier, Central_P1CRXWYI_USX"









