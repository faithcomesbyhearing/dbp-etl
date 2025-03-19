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
from MondayProductCodeBoard import MondayProductCodeBoard, ProductCodeColumns


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
		print("\n*** DBPLoadController:validate ***")
		validate = Validate(self.config, self.db)
		validate.process(inputFilesets)
		for inp in inputFilesets:
			print("DBPLoadController:validate. fileset: %s, csvFilename: %s " %(inp.filesetId, inp.csvFilename))
			if os.path.isfile(inp.csvFilename):
				print("DBPLoadController:validate. fileset: %s added to upload list" %(inp.filesetId))
				InputFileset.upload.append(inp)
			else:
				print("DBPLoadController:validate. fileset: %s not added added to upload list" %(inp.filesetId))
				RunStatus.set(inp.filesetId, False)
		Log.writeLog(self.config)

	def updateBibles(self):
		# this should only be called when uploading an xml file, meaning (a) we are not uploading content and (b) Blimp is not the system of record
		print("\n*** DBPLoadController:updateBibles ***")
		RunStatus.statusMap[RunStatus.BIBLE] = RunStatus.NOT_DONE

		dbOut = SQLBatchExec(self.config)
		bibles = UpdateDBPBiblesTable(self.config, self.db, dbOut, self.languageReader)
		bibles.process()
		#dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("bibles")
		RunStatus.set(RunStatus.BIBLE, success)
		return success		


	def upload(self, inputFilesets):
		print("\n*** DBPLoadController:upload ***")
		self.s3Utility.uploadAllFilesets(inputFilesets)
		dbOut = SQLBatchExec(self.config)
		secondary = UpdateDBPBibleFilesSecondary(self.config, self.db, dbOut)
		secondary.createAllZipFiles(inputFilesets)
		Log.writeLog(self.config)


	def updateFilesetTables(self, inputFilesets):
		print("\n*** DBPLoadController:updateFilesetTables ***")
		inp = inputFilesets
		dbOut = SQLBatchExec(self.config)
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut, self.languageReader)
		for inp in inputFilesets:
			print("DBPLoadController:updateFilesetTables. processing fileset: %s" % (inp.filesetId))
			hashId = update.processFileset(inp)

			if inp.typeCode == "text" and inp.subTypeCode() == "text_json":
				self.validateJSONFilesets(inp)
			dbOut.displayCounts()
			success = dbOut.execute(inp.batchName())
			if success and inp.typeCode == "video":
				# Given that we've divided this process into two transactions: 1. Update Filesets Tables and 2. Update Video Tables, it's necessary
				# to close the current database connection and establish a new one after the first transaction. This ensures that any subsequent
				# queries executed will have access to the latest changes pushed by the SQLBatchExec instance (dbOut). This is crucial because
				# SQLBatchExec uses its own database connection instance, which is distinct from the 'self.db' instance used by this DBPLoadController class.
				self.db.close()
				self.refreshDB()

				# A new instance of UpdateDBPVideoTables must be created for each fileset. This is because the constructor of the UpdateDBPVideoTables
				# class executes a select query to create a map of video_stream filesets. It's crucial that this map takes into account any new filesets
				# that have been created up to this point.
				video = UpdateDBPVideoTables(self.config, self.db, dbOut)
				video.processFileset(inp.filesetPrefix, inp.filenames(), hashId)
				dbOut.displayCounts()
				success = dbOut.execute(inp.batchName() + "-video")
			RunStatus.set(inp.filesetId, success)
			if success:
				InputFileset.complete.append(inp)
			else:
				print("********** Fileset Table %s Update Failed **********" % (inp.filesetId))


	def updateLPTSTables(self):
		# this should only be called when uploading an xml file, meaning (a) we are not uploading content and (b) Blimp is not the system of record
		print("\n*** DBPLoadController:updateLPTSTables ***")
		RunStatus.statusMap[RunStatus.LPTS] = RunStatus.NOT_DONE

		dbOut = SQLBatchExec(self.config)
		lptsDBP = UpdateDBPLPTSTable(self.config, dbOut, self.languageReader)
		lptsDBP.process()
		dbOut.displayStatements()
		dbOut.displayCounts()
		success = dbOut.execute("lpts")
		RunStatus.set(RunStatus.LPTS, success)
		return success

	def validateJSONFilesets(self, inp):
		result = self.db.selectSet("select filesetid, SUBSTR(filesetid, 8,1) as sub " +
			"from bible_fileset_lookup " +
			"WHERE type like %s " +
			"and length (filesetid) > 6 " +
			"and SUBSTR(filesetid, 8,1) != '_'", ('text%'))

		if len(result) > 0:
			logger = Log.getLogger(inp.filesetId)
			logger.invalidValues(inp.stockNum(), "text_json", len(result))

	def refreshDB(self):
		db = SQLUtility(self.config)
		self.db = db

	def synchronizeMonday(self, inputFileset):
		mondayService = MondayProductCodeBoard(self.config)
		(languageRecord, _) = self.languageReader.getLanguageRecordLoose(inputFileset.typeCode, inputFileset.bibleId, inputFileset.filesetId)
		stocknumber = languageRecord.StockNumberByFilesetId(inputFileset.filesetId)

		zipFiles = inputFileset.zipFilesIndexedByBookId()
		productCodes = {}
		gospelBookNameMap = self.db.selectMap("SELECT id, notes FROM books where book_group = 'Gospels'", None)
		for bookId in gospelBookNameMap.keys():
			zipFile = zipFiles.get(bookId)
			if zipFile != None:
				licensor = languageRecord.LicensorList()[0] if languageRecord.LicensorList() != None else ""
				if len(licensor) >= 3:
					(_, licensorName, _) = licensor
				else:
					licensorName = ""
				productCodes[GospelFilmStockNumberPrefix + stocknumber[2:] + "_" + bookId] = {
					ProductCodeColumns.StockNumber: stocknumber,
					ProductCodeColumns.Language: languageRecord.LangName().strip() if languageRecord.LangName() != None else "",
					ProductCodeColumns.BiblebrainLink: self.config.cdn_partner_base + zipFile.name,
					ProductCodeColumns.Licensor: licensorName,
					ProductCodeColumns.CoLicensor: languageRecord.CoLicensor().strip() if languageRecord.CoLicensor() != None else "",
					ProductCodeColumns.Version: languageRecord.Version(),
					ProductCodeColumns.LanguageCountry: languageRecord.Country(),
				}

		if productCodes != {}:
			mondayService.synchronize(productCodes)
			Log.getLogger(inputFileset.filesetId).message(Log.INFO, "Monday product codes synchronized")

if (__name__ == '__main__'):
	print("*** DBPLoadController *** ")
	from LanguageReaderCreator import LanguageReaderCreator
	config = Config()
	AWSSession.shared() # ensure AWSSession init
	db = SQLUtility(config)
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B")
	print("Migration Stage: %s " % migration_stage)
	if len(sys.argv) != 2:
		print("Processing Content Load")

		# load content will always point to BLIMP stage because the lpts.xml is no longer used
		languageReader = LanguageReaderCreator("BLIMP").create("")
		ctrl = DBPLoadController(config, db, languageReader)
		InputFileset.validate = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
		ctrl.repairAudioFileNames(InputFileset.validate)
		ctrl.validate(InputFileset.validate)
		hasError = False
		for inp in InputFileset.upload:
			logger = Log.getLogger(inp.filesetId)
			if logger.errorCount() > 0:
				print("")
				print("DBPLoadController:validate. fileset: %s has errors" %(inp.filesetId))
				print("")
				RunStatus.set(inp.filesetId, False)
				hasError = True
		if not hasError:
			ctrl.upload(InputFileset.upload)
			ctrl.updateFilesetTables(InputFileset.database)
			for inputFileset in InputFileset.complete:
				print("Completed: ", inputFileset.filesetId)
				ctrl.synchronizeMonday(inputFileset)
	else:
		print("Processing Metadata Load")
		if migration_stage == "BLIMP":
			print("cannot process because Blimp is the system of record")
		elif migration_stage == "C" or migration_stage == "B":
			print("Processing XML file")
			lpts_xml = config.filename_lpts_xml
			languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)
			ctrl = DBPLoadController(config, db, languageReader)
			ctrl.updateBibles()
			ctrl.updateLPTSTables()

	Log.writeLog(config)
	RunStatus.exit()

# Get current lpts-dbp.xml
# aws --profile DBP_DEV s3 cp s3://dbp-etl-upload-newdata-fiu49s0cnup1yr0q/lpts-dbp.xml /Volumes/FCBH/bucket_data/lpts-dbp.xml

#python3 load/DBPLoadController.py test s3://etl-development-input Spanish_N2SPNTLA_USX #works with refactor

# python3 load/DBPLoadController.py test s3://etl-development-input "French_N1 & O1 FRABIB_USX"
# for filesetid in FRNPDCN_ET-usx FRNPDFN_ET-usx FRNPDVN_ET-usx FRNPDCN_ET FRNPDFN_ET FRNPDVN_ET; do  python3 load/TestCleanup.py test $filesetid; done
# for filesetid in KANDPIN_ET-usx KANDPIO_ET-usx KANDPIN_ET KANDPIO_ET; do  python3 load/TestCleanup.py test $filesetid; done
# python3 load/TestCleanup.py test FRABIBN_ET-usx
# python3 load/TestCleanup.py test SPNTLAN_ET
# python3 load/TestCleanup.py test ABIWBTN_ET-usx




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


# time python3 load/TestCleanup.py test JAAJARN1DA
# time python3 load/DBPLoadController.py test s3://etl-development-input/ JAAJARN1DA

# time python3 load/TestCleanup.py test SPAERVN_ET
# time python3 load/TestCleanup.py test SPAERVN_ET-json
# time python3 load/TestCleanup.py test SPAERVN_ET-usx
# time python3 load/DBPLoadController.py test s3://etl-development-input/ Spanish_N1SPAPBT_USX


# time python3 load/TestCleanup.py test ENGESVN2DA
# time python3 load/TestCleanup.py test ENGESVN2DA-opus16
# time python3 load/DBPLoadController.py test s3://etl-development-input/ ENGESVN2DA

# time python3 load/DBPLoadController.py test s3://etl-development-input/ "Covenant_Manobo, Obo SD_S2OBO_COV"
# time python3 load/DBPLoadController.py test s3://etl-development-input/ "Covenant_Aceh SD_S2ACE_COV"

# time python3 load/DBPLoadController.py test s3://etl-development-input/ "SPNBDAP2DV"
