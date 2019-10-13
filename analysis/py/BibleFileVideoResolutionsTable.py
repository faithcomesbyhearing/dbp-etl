# BibleFileVideoResolutionsTable.py
#
#
#| id                | int(10) unsigned | NO   | PRI | NULL              | auto_increment |
#| bible_file_id     | int(10) unsigned | NO   | MUL | NULL              |                |
#| file_name         | varchar(191)     | NO   | UNI | NULL              |                |
#| bandwidth         | int(10) unsigned | NO   |     | NULL              |                |
#| resolution_width  | int(10) unsigned | NO   |     | NULL              |                |
#| resolution_height | int(10) unsigned | NO   |     | NULL              |                |
#| codec             | varchar(64)      | NO   |     |                   |                |
#| stream            | tinyint(1)       | NO   |     | NULL              |                |
#

import io
import os
import sys
from Config import *
from SQLUtility import *

## filename uniqueness is a problem, because filesets ending in 16 have the same file name
## as those that do not.


class BibleFileVideoResolutionsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db
		self.VIDEO_RESOLUTION_MAP = {"360": "640", "480": "854" , "720": "1280" }
		self.VIDEO_CODEC = "avc1.4d001f,mp4a.40.2"
		self.VIDEO_STREAM = "1"


	def process(self):
		self.readAll()
		results = []
		uniqueKeys = set()

		for file in self.filePathResults:
			bibleFileId = file[0]
			fileName = file[1]
			videoHeight = file[2]
			if videoHeight != None:
				bandwidth = self.bandwidth()
				videoWidth = self.videoWidth(videoHeight)
				codec = self.VIDEO_CODEC
				stream = self.VIDEO_STREAM
				if fileName not in uniqueKeys:
					uniqueKeys.add(fileName)
					results.append((bibleFileId, fileName, bandwidth, videoWidth, videoHeight, codec, stream))
				else:
					print("WARNING: dropped %s because duplicate" % (fileName))

		print("num records to insert %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bible_file_video_resolutions (bible_file_id, file_name, bandwidth, resolution_width, resolution_height, codec, stream) VALUES (%s, %s, %s, %s, %s, %s, %s)", results)


	def readAll(self):
		sql = ("SELECT f.id, b.file_name, b.video_height" +
			" FROM bible_files f, bucket_listing b" +
			" WHERE b.hash_id = f.hash_id"
			" AND b.book_id = f.book_id"
			" AND b.chapter_start = f.chapter_start"
			" AND b.verse_start = f.verse_start"
			" AND b.set_type_code = 'video_stream'")
		self.filePathResults = self.db.select(sql, None)
		print("num %d files in result table" % (len(self.filePathResults)))


	## Can I compute this given a 16:9 factor rounded up.
	def videoWidth(self, height):
		print("height", height)
		return self.VIDEO_RESOLUTION_MAP[height]


	## How should I compute this
	def bandwidth(self):
		return "0"


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
video = BibleFileVideoResolutionsTable(config, db)
video.process()
db.close() 



