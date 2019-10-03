# IdAnalysis.py

# 1) build table of bucket/type/bible/fileset from bucket and verses call analysis.bucket
# 2) build table of bible/fileset from LPTSReader call it analysis.lpts
# 3) build table of bucket/type/bible/fileset from dbp_only call it analysis.dbp_only
# 4) build table of bucket/type/bible/fileset from dbp call it analysis.dbp


import sys
from SQLUtility import *
from LPTSExtractReader import *
from VersesReader import *

class IdAnalysis:

	def __init__(self, config):
		self.config = config
		self.out = SQLUtility(config.database_host, config.database_port, config.database_user, "analysis")


	def createAnalysisBucket(self):
		self.privateCreateTable("buckets")
		sql = ("SELECT distinct fileset_id, bible_id, type_code, bucket FROM bucket_listing")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "valid_dbp")
		resultSet = db.select(sql, None)
		results = self.privateTransformType(resultSet)
		db.close()
		verses = VersesReader(self.config)
		verseIds = verses.bibleIdFilesetId()
		for verseId in verseIds:
			parts = verseId.split("/")
			results.append([parts[1], parts[0], "text_plain", "verses"])
		print("num %d insert in buckets" % (len(results)))
		self.out.executeBatch("INSERT INTO buckets VALUES (%s, %s, %s, %s)", results)


	def createAnalysisBucketNo16(self):
		self.out.execute("DROP VIEW IF EXISTS buckets_no16", None)
		sql = "CREATE VIEW buckets_no16 AS SELECT * FROM buckets WHERE right(fileset_id, 2) != '16'"
		self.out.execute(sql, None)


	def createAnalysisLPTS(self):
		self.privateCreateTable("lpts")
		results = []
		reader = LPTSExtractReader(self.config)
		self.privateAddFilesetId(results, reader.getAudioMap())
		self.privateAddFilesetId(results, reader.getTextMap())
		self.privateAddFilesetId(results, reader.getVideoMap())
		print("num %d insert in lpts" % (len(results)))
		self.out.executeBatch("INSERT INTO lpts VALUES (%s, %s, %s)", results)


	def privateAddFilesetId(self, results, filesetMap):
		for filesetId in filesetMap.keys():
			fileset = filesetMap[filesetId]
			if fileset.Reg_StockNumber() != None:
				stock_num = fileset.Reg_StockNumber()
			else:
				stock_num = "unknown"

			if fileset.DBP_Equivalent() != None:
				results.append([filesetId, fileset.DBP_Equivalent(), stock_num])
			if fileset.DBP_Equivalent2() != None:
				results.append([filesetId, fileset.DBP_Equivalent2(), stock_num])
			if fileset.DBP_Equivalent() == None and fileset.DBP_Equivalent2() == None:
				results.append([filesetId, "unknown", stock_num])



	def createAnalysisDBPOnly(self):
		self.privateCreateTable("dbp_only")
		sql = ("SELECT f.id, c.bible_id, f.set_type_code, f.asset_id"
				+ " FROM bible_filesets f, bible_fileset_connections c"
				+ " WHERE f.hash_id = c.hash_id")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "dbp_only")
		results = db.select(sql, None)
		db.close()
		print("num %d insert in dbp_only" % (len(results)))
		self.out.executeBatch("INSERT INTO dbp_only VALUES (%s, %s, %s, %s)", results)


	def createAnalysisDBP(self):
		self.privateCreateTable("dbp")
		sql = ("SELECT f.id, c.bible_id, f.set_type_code, f.asset_id"
				+ " FROM bible_filesets f, bible_fileset_connections c"
				+ " WHERE f.hash_id = c.hash_id")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "dbp")
		results = db.select(sql, None)
		db.close()
		print("num %d insert in dbp" % (len(results)))
		self.out.executeBatch("INSERT INTO dbp VALUES (%s, %s, %s, %s)", results)


	def createDiffViews(self):
		self.privateCreateDiffView("IN_buckets_NOT_IN_dbp_only_DIFF_bible_fileset", "buckets", "dbp_only", ["bible_id","fileset_id"])
		self.privateCreateDiffView("IN_buckets_NOT_IN_dbp_DIFF_bible_fileset", "buckets", "dbp", ["bible_id","fileset_id"])
		self.privateCreateDiffView("IN_buckets_NOT_IN_lpts_DIFF_bible_fileset", "buckets_no16", "lpts", ["bible_id","fileset_id"])

		self.privateCreateDiffView("IN_dbp_only_NOT_IN_buckets_DIFF_bible_fileset", "dbp_only", "buckets", ["bible_id","fileset_id"])
		self.privateCreateDiffView("IN_dbp_NOT_IN_buckets_DIFF_bible_fileset", "dbp", "buckets", ["bible_id","fileset_id"])
		self.privateCreateDiffView("IN_lpts_NOT_IN_buckets_DIFF_bible_fileset", "lpts", "buckets_no16", ["bible_id","fileset_id"])

		self.privateCreateDiffView("IN_buckets_NOT_IN_dbp_only_DIFF_fileset", "buckets", "dbp_only", ["fileset_id"])
		self.privateCreateDiffView("IN_buckets_NOT_IN_dbp_DIFF_fileset", "buckets", "dbp", ["fileset_id"])
		self.privateCreateDiffView("IN_buckets_NOT_IN_lpts_DIFF_fileset", "buckets_no16", "lpts", ["fileset_id"])

		self.privateCreateDiffView("IN_dbp_only_NOT_IN_buckets_DIFF_fileset", "dbp_only", "buckets", ["fileset_id"])
		self.privateCreateDiffView("IN_dbp_NOT_IN_buckets_DIFF_fileset", "dbp", "buckets", ["fileset_id"])
		self.privateCreateDiffView("IN_lpts_NOT_IN_buckets_DIFF_fileset", "lpts", "buckets_no16", ["fileset_id"])


	def close(self):
		self.out.close()


	def privateCreateTable(self, tableName):
		sql = "DROP TABLE IF EXISTS %s" % (tableName)
		self.out.execute(sql, None)
		if tableName == "lpts":
			sql = ("CREATE TABLE lpts ("
				+ " fileset_id varchar(255) not null,"
				+ " bible_id varchar(255) not null,"
				+ " stock_id varchar(255) not null,"
				+ " PRIMARY KEY(fileset_id, bible_id))")
		else:
			create = "CREATE TABLE %s (" % tableName
			sql = (create
				+ " fileset_id varchar(255) not null,"
				+ " bible_id varchar(255) not null,"
				+ " type_code varchar(255) not null,"	
				+ " bucket varchar(255) not null,"
				+ " PRIMARY KEY(fileset_id, bible_id, type_code, bucket))")
		print("SQL:", sql)
		self.out.execute(sql, None)


	def privateCreateDiffView(self, viewName, selectTable, compareTable, whereColumns):
		sql = "DROP VIEW IF EXISTS %s" % (viewName)
		self.out.execute(sql, None)
		parts = []
		for col in whereColumns:
			parts.append("a.%s=b.%s" % (col, col))
		joinClause = " AND ".join(parts)
		sql = ("CREATE VIEW %s AS SELECT a.* FROM %s a WHERE NOT EXISTS (SELECT 1 from %s b WHERE %s) ORDER BY fileset_id, bible_id" 
			% (viewName, selectTable, compareTable, joinClause))
		self.out.execute(sql, None)


	def privateTransformType(self, resultSet):
		results = []
		for row in resultSet:
			row = list(row)
			typeCode = row[2]
			if typeCode == "app":
				setTypeCode = "app"
			elif typeCode == "audio":
				filesetId = row[0]
				code = filesetId[7:10]
				if code == "1DA":
					setTypeCode = "audio"
				elif code == "2DA":
					setTypeCode = "audio_drama"
				else:
					print("WARNING: file type not known for %s, set_type_code set to 'unknown'" % (filesetId))
					setTypeCode = "unknown"
			elif typeCode == "text":
				setTypeCode = "text_format"
			elif typeCode == "video":
				setTypeCode = "video_stream"
			elif typeCode == "verse":
				setTypeCode = "text_plain"
			else:
				print("ERROR typeCode '%s' is not known" % (typeCode))
				sys.exit()
			row[2] = setTypeCode
			results.append(row)
		return results	


	def privateTransformType_obsolete(self, resultSet):
		results = []
		for row in resultSet:
			row = list(row)
			setTypeCode = row[2]
			if setTypeCode == "text_plain":
				typeCode = "verse"
			elif setTypeCode == "text_format":
				typeCode = "text"
			elif setTypeCode[:5] == "audio":
				typeCode = "audio"
			elif setTypeCode == "video_stream":
				typeCode = "video"
			else:
				typeCode = setTypeCode
			row[2] = typeCode
			results.append(row)
		return results


	def analyzeLPTSMismatches(self):
		lpts = LPTSExtractReader(self.config)
		bibleMap = lpts.getBibleIdMap()
		print("num bibleIds: %d" % (len(bibleMap.keys())))
		filesetMap = lpts.getAllFilesetMap()
		print("num filesetIds: %d" % (len(filesetMap.keys())))
		finalResults = []
		results = self.out.select("SELECT * FROM in_buckets_not_in_lpts_diff_bible_fileset", None)
		for row in results:
			filesetId = row[0]
			bibleId = row[1]
			typeCode = row[2]
			bucket = row[3]
			bibleLPTS = bibleMap.get(bibleId)
			filesetLPTS = filesetMap.get(filesetId)
			if bibleLPTS != None:
				bibleStockNum = bibleLPTS.Reg_StockNumber()
			else:
				bibleStockNum = None
			if filesetLPTS != None:
				filesetStockNum = filesetLPTS.Reg_StockNumber()
			else:
				filesetStockNum = None
			finalResults.append([filesetId, bibleId, typeCode, bucket, filesetStockNum, bibleStockNum])
			print(row, "filesetId StockNum: %s,  bibleId StockNum: %s" % (filesetStockNum, bibleStockNum))
		self.out.execute("DROP TABLE IF EXISTS lpts_errors", None)
		sql = ("CREATE TABLE lpts_errors ("
				+ " fileset_id varchar(255) not null,"
				+ " bible_id varchar(255) not null,"
				+ " type_code varchar(255) not null,"	
				+ " bucket varchar(255) not null,"
				+ " fileset_stock_num varchar(255) null,"
				+ " bible_stock_num varchar(255) null,"
				+ " PRIMARY KEY(fileset_id, bible_id, type_code, bucket))")
		self.out.execute(sql, None)
		self.out.executeBatch("INSERT INTO lpts_errors VALUES (%s, %s, %s, %s, %s, %s)", finalResults)


	def analyzeDBPMismatches(self):
		dbpAbsentees = self.out.select("SELECT * FROM in_buckets_not_in_dbp_diff_bible_fileset", None)
		for dbpAbsent in dbpAbsentees:
			filesetId = dbpAbsent[0]
			bibleId = dbpAbsent[1]
			typeCode = dbpAbsent[2]
			bucket = dbpAbsent[3]
			typeCd = typeCode.split("_")[0]
			#print(bucket, "%s/%s/%s/" % (typeCd, bibleId, filesetId)) # manually check in buckets
			biblesCount = self.out.selectScalar("SELECT count(*) FROM dbp.bibles WHERE id=%s", (bibleId))
			biblesConCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_fileset_connections WHERE bible_id=%s", (bibleId))
			if biblesCount == 0 and biblesConCount == 0:
				bibleMsg = "bible_id not in DBP"
			elif biblesCount == 0 and biblesConCount > 0:
				bibleMsg = "bible_id in connections, not bibles"
			elif biblesCount > 0 and biblesConCount == 0:
				bibleMsg = "bible_id not in connections"
			else:
				bibleMsg = ""
			filesetIdCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_filesets WHERE id=%s", (filesetId))
			if filesetIdCount == 0:
				filesetMsg = "fileset_id not in DBP"
			else:
				filesetMsg = ""
				filesetTypeCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_filesets WHERE id=%s AND set_type_code=%s", (filesetId, typeCode))
				filesetBktCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_filesets WHERE id=%s AND asset_id=%s", (filesetId, bucket))
				filesetAllCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_filesets WHERE id=%s AND asset_id=%s AND set_type_code=%s", (filesetId, bucket, typeCode))
				if filesetTypeCount > 0 and filesetBktCount == 0:
					filesetMsg = "fileset in wrong bucket"
				elif filesetTypeCount == 0 and filesetBktCount > 0:
					filesetMsg = "fileset has wrong type"
				elif filesetAllCount > 0:
					filesetMsg = "fileset/connections mismatch"
				else:
					"PUNT"
				print(bucket, typeCode, bibleId, filesetId, "%s  filesetType: %d  filesetBkt: %d  filesetAll: %d" % (bibleMsg, filesetTypeCount, filesetBktCount, filesetAllCount))
				#filesetConCount = self.out.selectScalar("SELECT count(*) FROM dbp.bible_fileset_connections WHERE")
			print(bucket, typeCode, bibleId, filesetId, "%s  %s" % (bibleMsg, filesetMsg))
			#manually verify the remaining extras.  But, document somehow



config = Config()
analysis = IdAnalysis(config)
#analysis.createAnalysisBucket()
#analysis.createAnalysisBucketNo16()
#analysis.createAnalysisLPTS()
#analysis.createAnalysisDBPOnly()
#analysis.createAnalysisDBP()
#analysis.createDiffViews()
#analysis.analyzeLPTSMismatches()
analysis.analyzeDBPMismatches()
analysis.close()




#select type_code, count(*) from buckets group by type_code;

#select type_code, count(*) from dbp group by type_code;

#select type_code, count(*) from dbp_only group by type_code;

