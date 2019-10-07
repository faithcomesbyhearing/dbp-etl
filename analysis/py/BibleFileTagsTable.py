# BibleFileTagsTable.py
#
#
#| file_id    | int(10) unsigned | NO   | PRI | NULL              |       |
#| tag        | varchar(12)      | NO   | PRI | NULL              |       |
#| value      | varchar(191)     | NO   |     | NULL              |       |
#| admin_only | tinyint(1)       | NO   |     | NULL              |       |
#
#

import io
import os
import sys
from Config import *
from SQLUtility import *
from mutagen.mp3 import MP3
from S3Utility import *


class BibleFileTagsTable:

	def __init__(self, config):
		self.config = config


	def readAll(self):
		self.validDB = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_output_db_name)
		sql = ("SELECT s.asset_id, b.bible_id, s.id, f.file_name, f.id" +
			" FROM bible_files f, bible_filesets s, bible_fileset_connections b" +
			" WHERE f.hash_id = s.hash_id" + 
			" AND s.hash_id = b.hash_id" +
			" AND f.hash_id = b.hash_id" +
			" AND s.set_type_code IN ('audio', 'audio_drama')")
		self.filePathResults = self.validDB.select(sql, None)
		print("num %d files in result table" % (len(self.filePathResults)))
		self.s3 = S3Utility(self.config)


	def fileId(self, row):
		return row[4]


	def tag(self):
		return "duration"


	def value(self, row):
		bucket = row[0]
		key = "audio/%s/%s/%s" % (row[1], row[2], row[3])
		filename = "tmp/" + key.replace("/", ":")
		self.s3.downloadFile(bucket, key, filename)
		try:
			audio = MP3(filename)
			print(audio)
			self.s3.deleteFile(filename)
			print(audio.info.length, str(int(round(audio.info.length))))
			return str(int(round(audio.info.length)))
		except Exception as err:
			print("WARNING: error %s on MP3 of %s" % (err, filename))
			return None


	def adminOnly(self):
		return 0


config = Config()
tags = BibleFileTagsTable(config) 
tags.readAll()
results = []

for file in tags.filePathResults:
	fileId = tags.fileId(file)
	tag = tags.tag()
	value = tags.value(file)
	adminOnly = tags.adminOnly()
	results.append((fileId, tag, value, adminOnly))
	if len(results) > 100:
		break

print("num records to insert %d" % (len(results)))
tags.validDB.close()
sys.exit()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%s, %s, %s, %s)", results)
outputDB.close()



