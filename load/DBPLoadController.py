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


class DBPLoadController:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.s3Utility = S3Utility(config)


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
		print("********** UPLOADING **********")
		self.s3Utility.uploadAllFilesets(config.s3_bucket)


	def updateDatabase(self):
		print("********** UPDATING **********")
		dbOut = SQLBatchExec(self.config)
		update = UpdateDBPFilesetTables(self.config, self.db, dbOut)
		filesetList = update.process()

		dbOut.displayStatements()
		dbOut.displayCounts()
		dbOut.execute()
		for filesetPrefix in filesetList:
			self.s3Utility.promoteFileset(self.config.directory_database, filesetPrefix)


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	ctrl = DBPLoadController(config, db, lptsReader)
	ctrl.validate()
	ctrl.upload()
	ctrl.updateDatabase()
	print("********** COMPLETE **********")



