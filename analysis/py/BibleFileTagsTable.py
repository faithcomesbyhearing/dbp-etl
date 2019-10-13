# BibleFileTagsTable.py
#
#
#| file_id    | int(10) unsigned | NO   | PRI | NULL              |       |
#| tag        | varchar(12)      | NO   | PRI | NULL              |       |
#| value      | varchar(191)     | NO   |     | NULL              |       |
#| admin_only | tinyint(1)       | NO   |     | NULL              |       |
#| PRIMARY KEY (file_id, tag)
#| FOREIGN KEY (file_id) REFERENCES bible_files (id)
#

import io
import os
import sys
from Config import *
from SQLUtility import *
from mutagen.mp3 import MP3
from S3Utility import *


class BibleFileTagsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()
		results = []

		for file in self.filePathResults:
			fileId = self.fileId(file)
			tag = self.tag()
			value = self.value(file)
			adminOnly = self.adminOnly()
			results.append((fileId, tag, value, adminOnly))
			if len(results) > 200:
				break

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%s, %s, %s, %s)", results)


	def readAll(self):
		sql = ("SELECT b.asset_id, b.bible_id, b.fileset_id, b.file_name, f.id" +
			" FROM bible_files f, bucket_listing b" +
			" WHERE b.hash_id = f.hash_id"
			" AND b.book_id = f.book_id"
			" AND b.chapter_start = f.chapter_start"
			" AND b.set_type_code IN ('audio', 'audio_drama')")
		self.filePathResults = self.db.select(sql, None)
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
			self.s3.deleteFile(filename)
			print(audio.info.length, str(int(round(audio.info.length))))
			return str(int(round(audio.info.length)))
		except Exception as err:
			print("WARNING: error %s on MP3 of %s" % (err, filename))
			return None


	def adminOnly(self):
		return 0


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
tags = BibleFileTagsTable(config, db)
tags.process()
db.close() 



