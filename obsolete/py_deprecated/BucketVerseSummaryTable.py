# BucketVerseSummaryTable.py

# This table contains a summary of the data in the bucket_listing table
# combined with a summary of the verses.

import io
import os
import sys
from Config import *
from Legacy import *
from SQLUtility import *
from VersesReader import *


class BucketVerseSummaryTable:


	def __init__(self, config, db):
		self.config = config
		self.legacy = Legacy(config)
		self.db = db


	def process(self):
		self.createSummaryTable()
		self.insertBucketSummary()
		self.insertVerseSummary()
		self.createIndexes()


	## The pkey assumes that a fileset can have multiple bibles.
	def createSummaryTable(self):
		self.db.execute("DROP TABLE IF EXISTS bucket_verse_summary", None)
		sql = ("CREATE TABLE bucket_verse_summary ("
			+ " fileset_id varchar(255) not null,"
			+ " asset_id varchar(255) not null,"
			+ " set_type_code varchar(255) not null,"
			+ " bible_id varchar(255) not null,"
			+ " legacy_asset_id varchar(255) not null,"
			+ " hash_id varchar(255) not null,"
			+ " PRIMARY KEY (fileset_id, asset_id, set_type_code, bible_id))")
		self.db.execute(sql, None)
		print("bucket_verse_summary created")


	def insertBucketSummary(self):
		sql = ("INSERT INTO bucket_verse_summary"
			+ " SELECT distinct fileset_id, asset_id, set_type_code, bible_id, legacy_asset_id, hash_id"
			+ " FROM bucket_listing")
		self.db.execute(sql, None)
		print("bucket rows inserted")


	def insertVerseSummary(self):
		results = []
		verses = VersesReader(self.config)
		bibleFilesets = verses.bibleIdFilesetId()
		for bibleFileset in bibleFilesets:
			parts = bibleFileset.split("/")
			bibleId = parts[0]
			filesetId = parts[1]
			setTypeCode = "text_plain"
			assetId = "db"
			legacyAssetId = self.legacy.legacyAssetId(filesetId, setTypeCode, assetId)
			hashId = self.legacy.hashId(legacyAssetId, filesetId, setTypeCode)
			results.append((filesetId, assetId, setTypeCode, bibleId, legacyAssetId, hashId))
		self.db.executeBatch("INSERT INTO bucket_verse_summary (fileset_id, asset_id, set_type_code, bible_id, legacy_asset_id, hash_id) VALUES (%s, %s, %s, %s, %s, %s)", results)
		print("num %d verse rows inserted" % (len(results)))

	def createIndexes(self):
		## This is redundant of primary key, if that is kept in
		self.db.execute("CREATE INDEX bucket_verse_summary_bible_id ON bucket_verse_summary (bible_id)", None)
		## If multiple bibles per fileset should not be allowed, then this can be unique 
		self.db.execute("CREATE INDEX bucket_verse_summary_bible_hash_id ON bucket_verse_summary (hash_id)", None)
		print("indexes created")


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
summary = BucketVerseSummaryTable(config, db)
summary.process()
db.close()





