# BibleFileVideoTransportStreamTable.py
#
#
#| id                  | int(10) unsigned | NO   | PRI | NULL              | auto_increment |
#| video_resolution_id | int(10) unsigned | NO   | MUL | NULL              |                |
#| file_name           | varchar(191)     | NO   | UNI | NULL              |                |
#| runtime             | double(8,2)      | NO   |     | NULL              |                |
#| PRIMARY KEY (id)
#| UNIQUE KEY (file_name)
#| KEY (video_resolution_id)
#| FOREIGN KEY (video_resolution_id) REFERENCES bible_file_video_resolutions (id)
#

import io
import os
import sys
from Config import *
from SQLUtility import *

## This is not finished.  It must read .ts files


class BibleFileVideoTransportStreamTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()
		results = []

		for file in self.filePathResults:
			videoId = file[0]
			fileName = file[1]
			runtime = self.runtime()
			results.append((videoId, fileName, runtime))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_file_video_transport_stream (video_resolution_id, file_name, runtime) VALUES (%s, %s, %s)", results)


	def readAll(self):
		sql = ("SELECT f.id, b.file_name, b.video_height" +
			" FROM bible_files f, bucket_listing b" +
			" WHERE b.hash_id = f.hash_id"
			" AND b.book_id = f.book_id"
			" AND b.chapter_start = f.chapter_start"
			" AND b.verse_start = f.verse_start"
			" AND b.set_type_code = 'video_stream'")

		self.filePathResults = self.db.select(sql, None)

		bucketPath = self.config.directory_bucket_list % (bucket.replace("-", "_"))
		file = io.open(bucketPath, mode="r", encoding="utf-8")
		for line in file:
			if line.endswith(".ts"):
				parts = line.split("/")


		print("num %d files in result table" % (len(self.filePathResults)))


	def runtime(self):
		print("height", height)
		return self.VIDEO_RESOLUTION_MAP[height]


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
video = BibleFileVideoTransportStreamTable(config, db)
video.process()
db.close() 



