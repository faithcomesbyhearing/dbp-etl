# UpdateDBPAccessTable.py


from Config import *
from LanguageReader import *
from SQLUtility import *
from SQLBatchExec import *


class UpdateDBPAccessTable:


	def __init__(self, config, db, dbOut, languageReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.languageReader = languageReader


	##
	## Access Group Filesets
	##
	def process(self, filesetList):
		insertRows = []
		deleteRows = []
		dbpAccessMapSet = self.db.selectMapSet("SELECT hash_id, access_group_id FROM access_group_filesets", ())
		audioAccessTypes = self.db.select("SELECT id, name, lpts_fieldname FROM access_groups WHERE name like %s", ("%audio%",))
		textAccessTypes = self.db.select("SELECT id, name, lpts_fieldname FROM access_groups WHERE name like %s", ("%text%",))
		videoAccessTypes = self.db.select("SELECT id, name, lpts_fieldname FROM access_groups WHERE name like %s", ("%video%",))
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			#print(bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId)
			typeCode = setTypeCode.split("_")[0]
			if filesetId[8:10] == "SA":
				dbpFilesetId = filesetId[:8] + "DA" + filesetId[10:]
			else:
				dbpFilesetId = filesetId
			dbpAccessSet = dbpAccessMapSet.get(hashId, set())

			if typeCode == "audio":
				accessTypes = audioAccessTypes
			elif typeCode == "text":
				accessTypes = textAccessTypes
			elif typeCode == "video":
				accessTypes = videoAccessTypes
			else:
				print("FATAL: Unknown typeCode % in fileset: %s, hashId: %s" % (typeCode, filesetId, hashId))
				sys.exit()

			(languageRecord, lptsIndex) = self.languageReader.getLanguageRecord(typeCode, bibleId, dbpFilesetId)
			if languageRecord != None:
				lpts = languageRecord.record
			else:
				lpts = {}

			for (accessId, accessName, accessDesc) in accessTypes:
				accessIdInDBP = accessId in dbpAccessSet;
				if accessId == 101: # allow_text_NT_DBP
					accessIdInLPTS = lpts.get(accessDesc) == "-1" and "NT" in setSizeCode
				elif accessId == 102: # allow_text_OT_DBP
					accessIdInLPTS = lpts.get(accessDesc) == "-1" and "OT" in setSizeCode
				elif accessId == 181: # allow_text_DOWNLOAD
					accessIdInLPTS = self._isPublicDomain(languageRecord)
				elif accessId in {191, 193}: # allow_text_APP_OFFLINE and allow_audio_APP_OFFLINE
					accessIdInLPTS = self._isSILOnly(languageRecord) or self._isPublicDomain(languageRecord)
				else:
					accessIdInLPTS = lpts.get(accessDesc) == "-1"

				if accessIdInLPTS and not accessIdInDBP:
					insertRows.append((hashId, accessId))
				elif accessIdInDBP and not accessIdInLPTS:
					deleteRows.append((hashId, accessId))

		tableName = "access_group_filesets"
		pkeyNames = ("hash_id", "access_group_id")
		self.dbOut.insert(tableName, pkeyNames, (), insertRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def _isSILOnly(self, record):
		if record != None:
			return record.Licensor() == "SIL" and record.CoLicensor() == None
		else:
			return False


	def _isPublicDomain(self, record):
		if record != None:
			pattern = re.compile(r"Public Domain", re.IGNORECASE)
			if record.Licensor() != None and pattern.search(record.Licensor()):
				return True
			if record.Copyrightc() != None and pattern.search(record.Copyrightc()):
				if len(record.Copyrightc()) < 20:
					return True
				else:
					print("INFO Possible Public Domain Found in %s: %s" % (record.Reg_StockNumber(), record.Copyrightc()))
		return False


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator	

	config = Config()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	filesets = UpdateDBPAccessTable(config, db, dbOut, languageReader)
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
	filesetList = db.select(sql, ())
	filesets.process(filesetList)
	db.close()
	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("test-lpts")

"""
CREATE TEMPORARY TABLE TWOS
SELECT hash_id FROM access_group_filesets WHERE access_group_id=2;

CREATE TEMPORARY TABLE ONEIGHTY
SELECT hash_id FROM access_group_filesets WHERE access_group_id=181;

SELECT * from bible_filesets where hash_id in (SELECT hash_id from TWOS where hash_id not in (select hash_id from ONEIGHTY))

SELECT * from bible_filesets where hash_id in (SELECT hash_id from ONEIGHTY where hash_id not in (select hash_id from TWOS))

select * from bible_filesets where hash_id in (select hash_id from access_group_filesets where access_group_id in (191, 193))
order by id
"""



