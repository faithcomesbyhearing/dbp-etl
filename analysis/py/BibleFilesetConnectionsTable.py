# BibleFilesetConnectionsTable.py
#
# There are only two records that have the same hash_id repeated with different bible_ids
#
#| hash_id    | char(12)    | NO   | PRI | NULL              |
#| bible_id   | varchar(12) | NO   | PRI | NULL              | 
#

# Get a list of filesetId's and hashIds from the database
# Get a map of the list of biblesId associated with each filesetId in the bucket

import io
import os
import sys
from Config import *
from SQLUtility import *
from VersesReader import *

class BibleFilesetConnectionsTable:

	def __init__(self, config):
		self.config = config
		validDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
		self.filesetList = validDB.select("SELECT id, set_type_code, hash_id FROM bible_filesets", None)
		print("num filesets in bible_filesets %d" % (len(self.filesetList)))
		self.appMap = validDB.selectMapList("SELECT distinct fileset_id, bible_id FROM bucket_listing WHERE type_code = 'app'", None)
		self.audioMap = validDB.selectMapList("SELECT distinct fileset_id, bible_id FROM bucket_listing WHERE type_code = 'audio'", None)
		self.textMap = validDB.selectMapList("SELECT distinct fileset_id, bible_id FROM bucket_listing WHERE type_code = 'text'", None)
		self.videoMap = validDB.selectMapList("SELECT distinct fileset_id, bible_id FROM bucket_listing WHERE type_code = 'video'", None)
		validDB.close()
		verses = VersesReader(config)
		self.verseMap = verses.filesetBibleMapList()
		print("num filesets in app: %d,  audio: %d,  text: %d,  video: %d  verses: %d" % (len(self.appMap.keys()), len(self.audioMap.keys()), len(self.textMap.keys()), len(self.videoMap.keys()), len(self.verseMap.keys())))


config = Config()
connects = BibleFilesetConnectionsTable(config)
results = []

for row in connects.filesetList:
	filesetId = row[0]
	typeCode = row[1]
	hashId = row[2]
	if typeCode == "app":
		bibleIds = connects.appMap.get(filesetId)
	elif typeCode == "audio" or typeCode == "audio_drama":
		bibleIds = connects.audioMap.get(filesetId)
	elif typeCode == "text_format":
		bibleIds = connects.textMap.get(filesetId)
	elif typeCode == "video_stream":
		bibleIds = connects.videoMap.get(filesetId)
	elif typeCode == "text_plain":
		bibleIds = connects.verseMap.get(filesetId)
	else:
		print("ERROR: unknown type_code %s" % (typeCode))
		sys.exit()

	if bibleIds != None:
		for bibleId in bibleIds:
			results.append((hashId, bibleId))
	else:
		print("WARNING: filesetId %s, type_code %s has no bibleIds" % (filesetId, typeCode))

print("num records to insert %d" % (len(results)))

outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_fileset_connections (hash_id, bible_id) VALUES (%s, %s)", results)
outputDB.close()



