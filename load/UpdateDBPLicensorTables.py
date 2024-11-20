# UpdateDBPLicensorTables.py
#
# This program inserts and replaces records in the DBP database
# 1. bible_fileset_copyright_organizations
# 2. bible_fileset_copyrights
#
# 1. Insert the transactions one fileset at a time.

from Config import Config
from SQLUtility import SQLUtility
from SQLBatchExec import SQLBatchExec
from BlimpLanguageService import getLicensorsByFilesetId, getCopyrightByFilesetId

class UpdateDBPLicensorTables:

	def __init__(self, db, dbOut):
		self.db = db
		self.dbOut = dbOut

	def processFileset(self, primaryFilesetId, derivedHashId):
		
		inserts = []					
		tableName = "bible_fileset_copyright_organizations"
		pkeyNames = ("hash_id", "organization_role")
		attrNames = ("organization_id",)

		licensors = getLicensorsByFilesetId(primaryFilesetId)

		for (organizationId, _, _) in licensors:
			orgIdExisting = self.db.selectScalar("\
				SELECT organizations.id\
				FROM organizations\
				INNER JOIN bible_fileset_copyright_organizations ON bible_fileset_copyright_organizations.organization_id = organizations.id\
				WHERE bible_fileset_copyright_organizations.organization_role = 2\
				AND bible_fileset_copyright_organizations.hash_id = %s\
				AND organizations.id = %s", (derivedHashId, organizationId)
			)

			if orgIdExisting == None:
				inserts.append((organizationId, derivedHashId, 2))


		self.dbOut.insert(tableName, pkeyNames, attrNames, inserts)

		inserts = []
		updates = []
		tableName = "bible_fileset_copyrights"
		pkeyNames = ("hash_id",)
		attrNames = ("copyright_date", "copyright", "copyright_description")

		copyrights = getCopyrightByFilesetId(primaryFilesetId)

		for (copyright, copyrightDate, copyrightDescription) in copyrights:
			hashIdExisting = self.db.selectScalar("\
				SELECT bible_fileset_copyrights.copyright,\
					bible_fileset_copyrights.copyright_date,\
					bible_fileset_copyrights.copyright_description,\
					bible_fileset_copyrights.open_access\
				FROM bible_fileset_copyrights\
				WHERE bible_fileset_copyrights.hash_id = %s", (derivedHashId,)
			)

			if hashIdExisting == None:
				inserts.append((copyrightDate, copyright, copyrightDescription, derivedHashId))
			else:
				updates.append((copyrightDate, copyright, copyrightDescription, derivedHashId))

		self.dbOut.insert(tableName, pkeyNames, attrNames, inserts)
		self.dbOut.updateCol(tableName, pkeyNames, updates)

		return True

## Unit Test
if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator	
	from InputFileset import InputFileset
	from DBPLoadController import DBPLoadController
	from UpdateDBPFilesetTables import UpdateDBPFilesetTables
	from InputProcessor import InputProcessor
	from AWSSession import AWSSession

	config = Config.shared()
	languageReader = LanguageReaderCreator("BLIMP").create("")
	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
	db = SQLUtility(config)
	ctrl = DBPLoadController(config, db, languageReader)	
	ctrl.validate(filesets)
	ctrl.upload(InputFileset.upload)

	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut, languageReader)
	for inp in InputFileset.upload:
		hashId = update.processFileset(inp)

		dbOut.displayStatements()
		dbOut.displayCounts()
		dbOut.execute("test-" + inp.filesetId)
