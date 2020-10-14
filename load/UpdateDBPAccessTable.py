# UpdateDBPAccessTable.py


from Config import *
from LPTSExtractReader import *
from SQLUtility import *
from SQLBatchExec import *


class UpdateDBPAccessTable:


	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader


	##
	## Access Group Filesets
	##
	def process(self, filesetList):
		insertRows = []
		deleteRows = []
		dbpAccessMapSet = self.db.selectMapSet("SELECT hash_id, access_group_id FROM access_group_filesets", ())
		audioAccessTypes = self.db.select("SELECT id, name, description FROM access_groups WHERE name like %s", ("%audio%",))
		textAccessTypes = self.db.select("SELECT id, name, description FROM access_groups WHERE name like %s", ("%text%",))
		videoAccessTypes = self.db.select("SELECT id, name, description FROM access_groups WHERE name like %s", ("%video%",))
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			#print(bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId)
			typeCode = setTypeCode.split("_")[0]
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

			(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
			if lptsRecord != None:
				lpts = lptsRecord.record
			else:
				lpts = {}

			for (accessId, accessName, accessDesc) in accessTypes:
				accessIdInDBP = accessId in dbpAccessSet;
				if accessId == 101: # allow_text_NT_DBP
					accessIdInLPTS = lpts.get(accessDesc) == "-1" and "NT" in setSizeCode
				elif accessId == 102: # allow_text_OT_DBP
					accessIdInLPTS = lpts.get(accessDesc) == "-1" and "OT" in setSizeCode
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


	## deprecated
	def insertAccessGroups(self):
		count = self.db.selectScalar("SELECT count(*) FROM access_groups WHERE id > 100", ())
		if count > 0:
			return
		sql = "INSERT INTO access_groups (id, name, description) VALUES (%s, %s, %s)"
		values = []
		values.append((101, "allow_text_NT_DBP", "DBPText"))
		values.append((102, "allow_text_OT_DBP", "DBPTextOT"))
		values.append((103, "allow_audio_DBP", "DBPAudio"))
		#values.append((105, "allow_video_DBP", NULL))

		values.append((111, "allow_text_WEB", "HubText"))
		values.append((113, "allow_audio_WEB", "DBPWebHub"))
		values.append((115, "allow_video_WEB", "WebHubVideo"))

		values.append((121, "allow_text_API", "APIDevText"))
		values.append((123, "allow_audio_API", "APIDevAudio"))
		values.append((125, "allow_video_API", "APIDevVideo"))

		values.append((131, "allow_text_APP", "MobileText"))
		values.append((133, "allow_audio_APP", "DBPMobile"))
		values.append((135, "allow_video_APP", "MobileVideo"))

		values.append((141, "allow_text_GBA", "GBN_Text"))
		values.append((143, "allow_audio_GBA", "GBN_Audio"))
		values.append((145, "allow_video_GBA", "GBN_Video"))

		values.append((153, "allow_audio_RADIO", "Streaming"))
		values.append((155, "allow_video_RADIO", "StreamingVideo"))

		values.append((163, "allow_audio_ITUNES", "ItunesPodcast"))
		values.append((165, "allow_video_ITUNES", "ItunesPodcastVideo"))

		values.append((173, "allow_audio_SALES", "FCBHStore"))
		values.append((175, "allow_video_SALES", "FCBHStoreVideo"))

		values.append((183, "allow_audio_DOWNLOAD", "Download"))
		self.db.executeBatch(sql, values)


	## deprecated
	def accessGroupSymmetricTest(self):
		sql = ("SELECT distinct bfc.bible_id, agf.access_group_id, ag.description"
			" FROM bible_fileset_connections bfc, bible_filesets bf, access_group_filesets agf, access_groups ag"
			" WHERE bfc.hash_id = bf.hash_id"
			" AND bf.hash_id = agf.hash_id"
			" AND agf.access_group_id = ag.id"
			" ORDER BY bfc.bible_id, ag.description")
		resultSet = self.db.select(sql, ())
		with open("test_DBP.txt", "w") as outFile:
			priorKey = None
			for (bibleId, accessGroupId, lptsName) in resultSet:
				key = bibleId
				if key != priorKey:
					outFile.write("<DBP_Equivalent>%s</DBP_Equivalent>\n" % (bibleId))
					priorKey = key
				outFile.write("\t<%s>-1</%s>\n" % (lptsName, lptsName))

		with open(self.config.filename_lpts_xml, "r") as lpts:
			results = {}
			permissions = set()
			hasLive = False
			for line in lpts:
				if "<DBP_Eq" in line:
					if len(permissions) > 0 and hasLive:
						results[bibleId] = permissions
					permissions = set()
					hasLive = False
					pattern = re.compile("<DBP_Equivalent[123]?>([A-Z0-9]+)</DBP_Equivalent[123]?>")
					found = pattern.search(line)
					if found != None:
						bibleId = found.group(1)
				if ">-1<" in line:
					permissions.add(line.strip())
				if ">Live<" in line or ">live<" in line:
					hasLive = True
		with open("test_LPTS.txt", "w") as outFile:
			for bibleId in sorted(results.keys()):
				outFile.write("<DBP_Equivalent>%s</DBP_Equivalent>\n" % (bibleId))
				for permission in sorted(results[bibleId]):
					outFile.write("\t%s\n" % (permission))


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	filesets = UpdateDBPAccessTable(config, db, dbOut, lptsReader)
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
	filesetList = db.select(sql, ())
	filesets.process(filesetList)
	db.close()
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute("test-lpts")



