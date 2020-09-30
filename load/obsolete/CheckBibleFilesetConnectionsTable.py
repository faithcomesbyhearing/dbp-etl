# CheckBibleFilesetConnectionsTable.py

# This class performs checks between DBP and LPTS for the contents of the bible_fileset_connections table.
# It does not update the table with differences found, because it is most important that this table
# is consistent with the contents of the bucket.

import io
import sys
from Config import *
from SQLUtility import *
from LPTSExtractReader import *

class CheckBibleFilesetConnectionsTable:


	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader

	##
	## Bible_Fileset_Connections
	##
	def process(self, filesetList):
		## Create DBP Map filesetId/typeCode: [(filesetId, setTypeCode, hashId, bibleId)]
		dbpFilesetListMap = {}
		for (bibleId, fid, setTypeCode, setSizeCode, assetId, hashId) in filesetList:			
			filesetId = fid[:10]
			if filesetId[8:10] == "SA":
				filesetId = filesetId[:8] + "DA" + filesetId[10:]
			typeCode = setTypeCode.split("_")[0]
			key = filesetId + "/" + typeCode
			values = dbpFilesetListMap.get(key, [])
			values.append((fid, setTypeCode, hashId, bibleId))
			dbpFilesetListMap[key] = values

		## Check for absent and multiple bibleId's within a DBP filesetId/typeCode group
		for key, filesets in dbpFilesetListMap.items():
			bibleIdSet = set()
			for (filesetId, setTypeCode, hashId, bibleId) in filesets:
				if bibleId == None or bibleId == "":
					print("ERROR_01 BibleId absent in DBP Record", key, filesets)
				else:
					bibleIdSet.add(bibleId)
			if len(bibleIdSet) > 1:
				print("ERROR_02 Multiple BibleId in DBP Fileset Group ", key, filesets)
		
		## Create LPTS Map filesetId/typeCode: (filesetId, typeCode, index, bibleId)
		indexMap = {"audio": [1, 2, 3], "text": [1, 2, 3], "video": [1]}
		lptsFilesetListMap = {}
		for lptsRecord in self.lptsReader.resultSet:
			for typeCode in ["audio", "text", "video"]:
				for index in indexMap[typeCode]:
					filesetIds = lptsRecord.DamIdMap(typeCode, index)
					for filesetId in filesetIds:
						key = filesetId + "/" + typeCode
						values = lptsFilesetListMap.get(key, [])
						bibleId = lptsRecord.DBP_EquivalentByIndex(index)
						values.append((filesetId, typeCode, index, bibleId))
						lptsFilesetListMap[key] = values
		#print(lptsFilesetListMap)

		## Check for absent and multiple bibleId's within a LPTS filesetId/typeCode group
		for key, filesets in lptsFilesetListMap.items():
			bibleIdSet = set()
			for (filesetId, typeCode, index, bibleId) in filesets:
				if bibleId == None or bibleId == "":
					print("ERROR_03 BibleId absent in LPTS Record", key, filesets)
				else:
					bibleIdSet.add(bibleId)
			if len(bibleIdSet) > 1:
				print("ERROR_04 Multiple BibleId in Fileset Group ", key, filesets)

		## Compare two maps for missing DBP records, and issue warnings
		for key in lptsFilesetListMap.keys():
			if dbpFilesetListMap.get(key) == None:
				print("WARN_05 DBP Missing", key, lptsFilesetListMap[key])

		## Compare two maps for missing LPTS filesets, and issue warnings
		for key in dbpFilesetListMap.keys():
			if lptsFilesetListMap.get(key) == None:
				print("WARN_06 LPTS Missing", key, dbpFilesetListMap[key])

		## Compare two match for key matches, and compare Bible Id
		for key, lptsFilesets in lptsFilesetListMap.items():
			dbpFilesets = dbpFilesetListMap.get(key)
			if dbpFilesets != None:
				dbpBibleIdSet = set()
				for (filesetId, setTypeCode, hashId, bibleId) in dbpFilesets:
					if bibleId != None:
						dbpBibleIdSet.add(bibleId)
				lptsBibleIdSet = set()
				for (filesetId, typeCode, index, bibleId) in lptsFilesets:
					if bibleId != None:
						lptsBibleIdSet.add(bibleId)
				if dbpBibleIdSet != lptsBibleIdSet:
					print("ERROR_07 Bibles not the same in LPTS and DBP")
					print("\tLPTS: ", lptsFilesets, "\n\tDBP:", dbpFilesets)

		#tableName = "bible_fileset_connections"
		#pkeyNames = ("hash_id", "bible_id")
		#attrNames = ()
	

## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	lptsReader = LPTSExtractReader(config)
	bibles = CheckBibleFilesetConnectionsTable(config, db, lptsReader)
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
	filesetList = db.select(sql, ())
	bibles.process(filesetList)
	db.close()