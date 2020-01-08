# UpdateDBPVideoTables.py

# This program loads m3u8 files and ts files found in the dbp-vid into the DBP database.
# This program is used as part of a database load from a bucket, but it is not used
# for a typical incremental load of data from files that uploaded.  For an incremental
# load these files are created in the bucket by an elasticencoder.

import sys
import os
import re
from Config import *
from SQLUtility import *

Should I build a map of quarantine, and a map of duplicates from the csv files
Then when I do not find find a file in the database, but I do find it in duplicates
or quarantine, I can skip it without error.
Otherwise, I should stop.

The above logic only works for a full load, but I think that is the only time this program is run.


class UpdateDBPVideoTables:

	def __init__(self, config):
		self.config = config
		self.db = SQLUtility(config.database_host, config.database_port, config.database_user, config.database_db_name)
		sql = ("SELECT bf.id, bfc.bible_id, bs.id, bf.file_name"
			" FROM bible_files bf, bible_filesets bs, bible_fileset_connections bfc"
			" WHERE bf.hash_id = bs.hash_id AND bfc.hash_id = bs.hash_id"
			" AND bs.set_type_code = 'video_stream'")
		resultSet = self.db.select(sql, ())
		self.fileIdMap = {}
		for row in resultSet:
			filename = row[3].split(".")[0]
			key = "video/%s/%s/%s" % (row[1], row[2], filename)
			self.fileIdMap[key] = row[0]


	def generateStatements(self):
		m3u8Insert = ("INSERT INTO bible_file_stream_bandwidths (bible_file_id, file_name,"
			" bandwidth, resolution_width, resolution_height, codec, stream) VALUES"
			" (%d, '%s', %d, %d, %d, 'avc1.4d001f,mp4a.40.2', 1);")
		idLookup = "SELECT @bandwidth_id = lastrowid();"
		tsInsert = ("INSERT INTO bible_file_stream_ts (stream_bandwidth_id, file_name,"
			" runtime) VALUES (@bandwidth_id, '%s', %d);")

		statements = []
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
							print("ERROR: FileId is Null", line)
							#sys.exit()

						elif fileType == "m3u8":
							if not filename.endswith("_stream.m3u8"):
								bandwidth = self.getBandwidth()
								resolutionHeight = self.getHeight(line)
								resolutionWidth = self.getWidth(resolutionHeight)
								sql = m3u8Insert % (fileId, filename, bandwidth, resolutionWidth, resolutionHeight)
								statements.append(sql)
								statements.append(idLookup)

						elif fileType == "ts":
							runtime = self.getRuntime()
							sql = tsInsert % (filename, runtime)
							statements.append(sql)
					elif fileType not in {"mp4", "db", "jpg", "tif", "mp3", "png"}:
						print("Filetype is not expected", fileType, line)
						sys.exit()

		fp.close()
		for line in statements:
			print(line)


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


config = Config("dev")
update = UpdateDBPVideoTables(config)
m3u8Files = update.generateStatements()

"""
video/ACCIBS/ACCIBSP2DV/Achi_de_Cubulco_MRK_14-32-52.mp4
video/ACCIBS/ACCIBSP2DV/Achi_de_Cubulco_MRK_14-32-52_av360p.m3u8
video/ACCIBS/ACCIBSP2DV/Achi_de_Cubulco_MRK_14-32-52_av360p00000.ts
video/ACCIBS/ACCIBSP2DV/Achi_de_Cubulco_MRK_14-32-52_av360p00001.ts

video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13.mp4
video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13_av480p.m3u8
video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13_av480p00000.ts

video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13_av720p.m3u8
video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13_av720p00000.ts
video/ABIWBT/ABIWBTP2DV/Abidji_MRK_1-1-13_av720p00001.ts
"""