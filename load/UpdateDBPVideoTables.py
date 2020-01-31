# UpdateDBPVideoTables.py

# This program loads m3u8 files and ts files found in the dbp-vid into the DBP database.
# This program is used as part of a database load from a bucket, but it is not used
# for a typical incremental load of data from files that uploaded.  For an incremental
# load these files are created in the bucket by an elasticencoder.

import sys
import os
import re
import subprocess
from Config import *
from SQLUtility import *

VIDEO_STMTS_FILE = "VideoStmts.sql"


class UpdateDBPVideoTables:

	def __init__(self, config):
		self.config = config
		self.db = SQLUtility(config)
		sql = ("SELECT bf.id, bfc.bible_id, bs.id, bf.file_name"
			" FROM bible_files bf, bible_filesets bs, bible_fileset_connections bfc"
			" WHERE bf.hash_id = bs.hash_id AND bfc.hash_id = bs.hash_id"
			" AND bs.set_type_code = 'video_stream'")
		resultSet = self.db.select(sql, ())
		self.fileIdMap = {}
		for row in resultSet:
			fname = row[3]
			filePrefix = fname[:fname.rfind("_")]
			key = "video/%s/%s/%s" % (row[1], row[2], filePrefix)
			self.fileIdMap[key] = row[0]
		print("Num Video Files", len(resultSet))


	def generateStatements(self):
		m3u8Insert = ("INSERT INTO bible_file_stream_bandwidths (bible_file_id, file_name,"
			" bandwidth, resolution_width, resolution_height, codec, stream) VALUES"
			" (%d, '%s', %d, %d, %d, 'avc1.4d001f,mp4a.40.2', 1);\n")
		idLookup = "SET @bandwidth_id = LAST_INSERT_ID();\n"
		tsInsert = ("INSERT INTO bible_file_stream_ts (stream_bandwidth_id, file_name,"
			" runtime) VALUES (@bandwidth_id, '%s', %d);\n")

		notProcessedSet = set()
		statements = open(VIDEO_STMTS_FILE, "w")
		fp = open(config.directory_bucket_list + os.sep + self.config.s3_vid_bucket + ".txt", "r")
		for line in fp:
			if line.startswith("video/"):
				line = line.split("\t")[0]
				#print("line", line)
				if "." in line:
					fileType = line.split(".", 2)[-1]
					if len(fileType) > 5:
						print("Filetype is too long", line)
						sys.exit()
					if fileType == "m3u8" or fileType == "ts":
						lastUnderscore = line.rfind("_")
						filePathKey = line[:lastUnderscore]
						#print("filePathKey", filePathKey)
						filename = line.split("/")[-1]
						fileId = self.fileIdMap.get(filePathKey)
						if fileId == None:
							if fileType == "m3u8":
								filesetId = line.split("/")[2]
								if not filesetId.endswith("DA") and not filesetId.endswith("DA16"):
									notProcessedSet.add(line)

						elif fileType == "m3u8":
							if not filename.endswith("_stream.m3u8"):
								bandwidth = self.getBandwidth()
								resolutionHeight = self.getHeight(line)
								resolutionWidth = self.getWidth(resolutionHeight)
								sql = m3u8Insert % (fileId, filename, bandwidth, resolutionWidth, resolutionHeight)
								statements.write(sql)
								statements.write(idLookup)

						elif fileType == "ts":
							runtime = self.getRuntime()
							sql = tsInsert % (filename, runtime)
							statements.write(sql)
					elif fileType not in {"mp4", "db", "jpg", "tif", "mp3", "png"}:
						print("Filetype is not expected", fileType, line)
						sys.exit()

		fp.close()
		statements.close()
		#for line in statements:
		#	print(line)
		print(len(notProcessedSet), "NOT PROCESSED")
		for line in sorted(list(notProcessedSet)):
			print(line)
		print(len(notProcessedSet))


	## to be done.  How?
	def getBandwidth(self):
		return 0


	def getHeight(self, filePath):
		pattern = re.compile(".*_av([0-9]{3,4})p.m3u8")
		result = pattern.match(filePath)
		if result == None:
			print("No match for resolution", filePath)
			sys.exit()
		else:
			return int(result.group(1))


	def getWidth(self, height):
		if height == 360:
			return 640
		elif height == 480:
			return 854
		elif height == 720:
			return 1280
		else:
			print("Unexpected resolution height", height)
			sys.exit()


    ## to be done.  How?
	def getRuntime(self):
		return 0


	def executeBatch(self, filename):
		cmd = ("mysql -u%s -p%s  %s <  %s" % 
			(self.config.database_user, self.config.database_passwd, self.config.database_db_name, filename))
		try:
			response = subprocess.run(cmd, shell=True, capture_output=True)
			print(response)
		except Exception as err:
			print(err)
		## what happens with bad sql
		## what happens with duplicate key error



config = Config("dev")
update = UpdateDBPVideoTables(config)
update.generateStatements()
update.executeBatch(VIDEO_STMTS_FILE)

