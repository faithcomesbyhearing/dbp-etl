# IdAnalysis.py

# 1) build table of bucket/type/bible/fileset from bucket and verses call analysis.bucket
# 2) build table of bible/fileset from LPTSReader call it analysis.lpts
# 3) build table of bucket/type/bible/fileset from dbp_only call it analysis.dbp_only
# 4) build table of bucket/type/bible/fileset from dbp call it analysis.dbp


from SQLUtility import *
from LPTSExtractReader import *
from VersesReader import *

class IdAnalysis:

	def __init__(self, config):
		self.config = config
		self.out = SQLUtility(config.database_host, config.database_port, config.database_user, "analysis")


	def createAnalysisBucket(self):
		self.privateCreateTable("buckets")
		sql = ("SELECT distinct bucket, type_code, bible_id, fileset_id FROM bucket_listing")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "valid_dbp")
		resultSet = db.select(sql, None)
		results = list(resultSet)
		db.close()
		verses = VersesReader(self.config)
		verseIds = verses.bibleIdFilesetId()
		for verseId in verseIds:
			parts = verseId.split("/")
			results.append(["verses", "verse", parts[0], parts[1]])
		print("num %d insert in buckets" % (len(results)))
		self.out.executeBatch("INSERT INTO buckets VALUES (%s, %s, %s, %s)", results)


	def createAnalysisLPTS(self):
		self.privateCreateTable("lpts")
		results = []
		reader = LPTSExtractReader(self.config)
		self.privateAddFilesetId(results, reader.getAudioMap())
		self.privateAddFilesetId(results, reader.getTextMap())
		self.privateAddFilesetId(results, reader.getVideoMap())
		print("num %d insert in lpts" % (len(results)))
		for re in results:
			print(re)
		self.out.executeBatch("INSERT INTO lpts VALUES (%s, %s)", results)


	def privateAddFilesetId(self, results, filesetMap):
		for filesetId in filesetMap.keys():
			fileset = filesetMap[filesetId]
			if fileset.DBP_Equivalent() != None:
				results.append([fileset.DBP_Equivalent(), filesetId])
			if fileset.DBP_Equivalent2() != None:
				results.append([fileset.DBP_Equivalent2(), filesetId])
			if fileset.DBP_Equivalent() == None and fileset.DBP_Equivalent2() != None:
				results.append([None, filesetId])



	def createAnalysisDBPOnly(self):
		self.privateCreateTable("dbp_only")
		sql = ("SELECT f.asset_id, f.set_type_code, c.bible_id, f.id"
				+ " FROM bible_filesets f, bible_fileset_connections c"
				+ " WHERE f.hash_id = c.hash_id")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "dbp_only")
		results = db.select(sql, None)
		db.close()
		results = self.privateTransformType(results)
		print("num %d insert in dbp_only" % (len(results)))
		self.out.executeBatch("INSERT INTO dbp_only VALUES (%s, %s, %s, %s)", results)


	def createAnalysisDBP(self):
		self.privateCreateTable("dbp")
		sql = ("SELECT f.asset_id, f.set_type_code, c.bible_id, f.id"
				+ " FROM bible_filesets f, bible_fileset_connections c"
				+ " WHERE f.hash_id = c.hash_id")
		db = SQLUtility(config.database_host, config.database_port, config.database_user, "dbp")
		results = db.select(sql, None)
		db.close()
		results = self.privateTransformType(results)
		print("num %d insert in dbp" % (len(results)))
		self.out.executeBatch("INSERT INTO dbp VALUES (%s, %s, %s, %s)", results)


	def close(self):
		self.out.close()


	def privateCreateTable(self, tableName):
		sql = "DROP TABLE IF EXISTS %s" % (tableName)
		self.out.execute(sql, None)
		if tableName == "lpts":
			sql = ("CREATE TABLE lpts ("
				+ " bible_id varchar(255) null,"
				+ " fileset_id varchar(255) not null)")
		else:
			sql = ("CREATE TABLE %s ("
				+ " bucket varchar(255) not null,"
				+ " type_code varchar(255) not null,"
				+ " bible_id varchar(255) not null,"
				+ " fileset_id varchar(255) not null)") % (tableName)
			print("SQL", sql)
		self.out.execute(sql, None)


	def privateTransformType(self, resultSet):
		results = []
		for row in resultSet:
			row = list(row)
			setTypeCode = row[1]
			if setTypeCode == "text_plain":
				typeCode = "verse"
			elif setTypeCode[:5] == "audio":
				typeCode = "audio"
			else:
				typeCode = setTypeCode
			row[1] = typeCode
			results.append(row)
		return results


config = Config()
analysis = IdAnalysis(config)
analysis.createAnalysisBucket()
analysis.createAnalysisLPTS()
analysis.createAnalysisDBPOnly()
analysis.createAnalysisDBP()
analysis.close()








