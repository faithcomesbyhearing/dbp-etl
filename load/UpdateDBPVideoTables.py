# UpdateDBPVideoTables.py

# This program updates the bible_file_stream_bandwidth, and bible_file_stream_ts files for video

import sys
import os
import re
import subprocess
from Config import *
from SQLUtility import *
from SQLBatchExec import *


class UpdateDBPVideoTables:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		sql = ("SELECT file_name, id FROM bible_files where hash_id in"
			" (SELECT hash_id FROM bible_filesets WHERE set_type_code = 'video_stream')")
		self.fileIdMap = db.selectMap(sql, ())
		self.streamRegex = re.compile(r"BANDWIDTH=([0-9]+),RESOLUTION=([0-9]+)x([0-9]+),CODECS=\"(.+)\"")
		self.tsRegex = re.compile(r"#EXTINF:([0-9\.]+),")
		self.dbpBandwidthMap = None
		self.dbpTsFileMap = None


	def process(self):
		directory = self.config.directory_database
		for typeCode in os.listdir(directory):
			if typeCode == "video":
				for bibleId in [f for f in os.listdir(directory + typeCode) if not f.startswith('.')]:
					for filesetId in [f for f in os.listdir(directory + typeCode + os.sep + bibleId) if not f.startswith('.')]:
						self.populateDBPMaps(filesetId)
						filesetPrefix = typeCode + "/" + bibleId + "/" + filesetId + "/"
						done = self.updateVideoFileset(directory, filesetPrefix)
						print(filesetPrefix)
						if done:
							print("Update video %s succeeded." % (filesetPrefix))
						else:
							print("Update video %s FAILED." % (filesetPrefix))


	def populateDBPMaps(self, filesetId):
		sql = ("SELECT bible_file_id, bandwidth, resolution_width, resolution_height, codec, stream, file_name"
			" FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id IN"
			" (SELECT hash_id FROM bible_filesets WHERE id='%s' AND set_type_code LIKE 'video%'))")
		resultSet = self.db.select(sql, (filesetId,))
		self.dbpBandwidthMap = {}
		for (bibleFileId, bandwidth, resolutionWidth, resolutionHeight, codec, stream, fileName) in resultSet:
			self.dbpBandwidthMap[fileName] = (bibleFileId, bandwidth, resolutionWidth, resolutionHeight, codec, stream)

		sql = ("SELECT stream_bandwidth_id, runtime, file_name FROM bible_file_stream_ts WHERE stream_bandwidth_id IN"
			" (SELECT id FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id IN"
			" (SELECT hash_id FROM bible_filesets WHERE id='%s' AND set_type_code LIKE 'video%')))")
		resultSet = self.db.select(sql, (filesetId,))
		self.dbpTsFileMap = {}
		for (streamBandwidthId, runtime, fileName) in resultSet:
			self.dbpTsFileMap[fileName] = (streamBandwidthId, runtime)


	def updateVideoFileset(self, directory, filesetPrefix):
		errorCount = 0
		(typeCode, bibleId, filesetId) = filesetPrefix.split("/")
		for filename in [f for f in os.listdir(directory + filesetPrefix) if not f.startswith('.')]:
			m3u8Files = self.downloadM3U8(filesetPrefix, filename)
			for (m3u8Filename, m3u8Content) in m3u8Files.items():
				if m3u8Filename.endswith("_stream.m3u8"):
					self.processStreamM3U8(m3u8Filename, m3u8Content)
				else:
					self.processTSM3U8(m3u8Filename, m3u8Content)
		return errorCount == 0


	## To be rewritten as actual download
	def downloadM3U8(self, filesetPrefix, filename):
		m3u8Files = {}
		#suffixes = ["_stream", "_av720p", "_av480p", "_av360p"]
		suffixes = TranscodeVideo.getHLSTypes()
		name = filename.split(".")[0]
		for suffix in suffixes:
			m3u8Filename = name + suffix + ".m3u8"
			with open("/Volumes/FCBH/video_m3u8/" + filesetPrefix + m3u8Filename, "r") as m3u8:
				m3u8Files[m3u8Filename] = m3u8.read()
		return m3u8Files


	def processStreamM3U8(self, m3u8Filename, m3u8Content):
		insertRows = []
		updateRows = []
		deleteRows = []
		insertUpdateSet = set()

		fileId = self.fileIdMap.get(m3u8Filename)
		if fileId == None:
			fileId = "@" + m3u8Filename.replace("-", "_")
		for line in m3u8Content.split("\n"):
			if line.startswith("#EXT-X-STREAM-INF:"):
				match = self.streamRegex.search(line)
				bandwidth = match.group(1)
				width = match.group(2)
				height = match.group(3)
				codec = match.group(4)
				stream = 1
			elif line.endswith("m3u8"):
				filename = line

				if filename not in self.dbpBandwidthMap.keys():
					insertUpdateSet.add(filename)
					insertRows.append((fileId, bandwidth, width, height, codec, stream, filename))
				else:
					(dbpFileId, dbpBandwidth, dbpWidth, dbpHeight, dbpCodec, dbpStream) = dbpBandwidthMap[filename]
					if (fileId != dbpFileId or
						bandwidth != dbpBandwidth or
						width != dbpWidth or
						height != dbpHeight or
						codec != dbpCodec or
						stream != dbpStream):
						insertUpdateSet.add(filename)
						updateRows.append((fileId, bandwidth, width, height, codec, stream, filename))

		for dbpFilename in self.dbpBandwidthMap.keys():
			if dbpFilename not in insertUpdateSet:
				deleteRows.append((dbpFilename,))

		tableName = "bible_file_stream_bandwidths"
		pkeyNames = ("file_name",)
		attrNames = ("bible_file_id", "bandwidth", "resolution_width", "resolution_height", "codec", "stream")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows, 6)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def processTSM3U8(self, m3u8Filename, m3u8Content):
		insertRows = []
		updateRows = []
		deleteRows = []
		insertUpdateSet = set()

		bandwidthId = "@" + m3u8Filename.replace("-", "_")
		for line in m3u8Content.split("\n"):
			if line.startswith("#EXTINF:"):
				match = self.tsRegex.match(line)
				runtime = match.group(1)
			elif line.endswith("ts"):
				filename = line

				if filename not in self.dbpTsFileMap.keys():
					insertUpdateSet.add(filename)
					insertRows.append((bandwidthId, runtime, filename))
				else:
					(dbpBandwidthId, dbpRuntime) = self.dbpTsFileMap[filename]
					if (bandwidthId != dbpBandwidthId or
						runtime != dbpRuntime):
						insertUpdateSet.add(filename)
						updateRows.append((bandwidthId, runtime, filename))

		for dbpFilename in self.dbpTsFileMap.keys():
			if dbpFilename not in insertUpdateSet:
				deleteRows.append((dbpFilename,))

		tableName = "bible_file_stream_ts"
		pkeyNames = ("file_name",)
		attrNames = ("video_resolution_id", "runtime") ### bandwidth_id is required here
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)



if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut)
	update.process()
	video = UpdateDBPVideoTables(config, db, dbOut)
	video.process()
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute()


"""
# view: bible_file_video_resolutions
actual table: bible_file_stream_bandwidths
unique key: file_name
| id                | int(10) unsigned | NO   |     | 0                   |       |
| bible_file_id     | int(10) unsigned | NO   |     | NULL                |       |
| file_name         | varchar(191)     | NO   |     | NULL                |       |
| bandwidth         | int(10) unsigned | NO   |     | NULL                |       |
| resolution_width  | int(10) unsigned | YES  |     | NULL                |       |
| resolution_height | int(10) unsigned | YES  |     | NULL                |       |
| codec             | varchar(64)      | NO   |     |                     |       |
| stream            | tinyint(1)       | NO   |     | NULL                |       |
stream.m3u8
#EXTM3U
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=12115000,RESOLUTION=1280x720,CODECS="avc1.4d001f,mp4a.40.2"
English_ESV_MRK_1-1-13_av720p.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=6446000,RESOLUTION=854x480,CODECS="avc1.4d001f,mp4a.40.2"
English_ESV_MRK_1-1-13_av480p.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=4008000,RESOLUTION=640x360,CODECS="avc1.4d001f,mp4a.40.2"
English_ESV_MRK_1-1-13_av360p.m3u8
+--------+---------------+------------------------------------+-----------+------------------+-------------------+-----------------------+--------+---------------------+---------------------+
| id     | bible_file_id | file_name                          | bandwidth | resolution_width | resolution_height | codec                 | stream | created_at          | updated_at          |
+--------+---------------+------------------------------------+-----------+------------------+-------------------+-----------------------+--------+---------------------+---------------------+
| 210681 |       2120868 | English_ESV_MRK_1-1-13_av720p.m3u8 |  12115000 |             1280 |               720 | avc1.4d001f,mp4a.40.2 |      1 | 2020-01-15 15:48:02 | 2020-01-15 15:48:02 |
| 210682 |       2120868 | English_ESV_MRK_1-1-13_av480p.m3u8 |   6446000 |              854 |               480 | avc1.4d001f,mp4a.40.2 |      1 | 2020-01-15 15:48:02 | 2020-01-15 15:48:02 |
| 210683 |       2120868 | English_ESV_MRK_1-1-13_av360p.m3u8 |   4008000 |              640 |               360 | avc1.4d001f,mp4a.40.2 |      1 | 2020-01-15 15:48:02 | 2020-01-15 15:48:02 |
+--------+---------------+------------------------------------+-----------+------------------+-------------------+-----------------------+--------+---------------------+---------------------+

#bible_file_video_transport_stream
| id                  | int(10) unsigned | NO   |     | 0                   |       |
| video_resolution_id | int(10) unsigned | NO   |     | NULL                |       |
| file_name           | varchar(191)     | NO   |     | NULL                |       |
| runtime             | double(8,2)      | NO   |     | NULL                |       |
360p.m3u8
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-ALLOW-CACHE:YES
#EXT-X-TARGETDURATION:4
#EXTINF:3.083333,
English_ESV_MRK_1-1-13_av360p00000.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00001.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00002.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00003.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00004.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00005.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00006.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00007.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00008.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00009.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00010.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00011.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00012.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00013.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00014.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00015.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00016.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00017.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00018.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00019.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00020.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00021.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00022.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00023.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00024.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00025.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00026.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00027.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00028.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00029.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00030.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00031.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00032.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00033.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00034.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00035.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00036.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00037.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00038.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00039.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00040.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00041.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00042.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00043.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00044.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00045.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00046.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00047.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00048.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00049.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00050.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00051.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00052.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00053.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00054.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00055.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00056.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00057.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00058.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00059.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00060.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00061.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00062.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00063.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00064.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00065.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00066.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00067.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00068.ts
#EXTINF:3.000000,
English_ESV_MRK_1-1-13_av360p00069.ts
#EXTINF:1.458333,
English_ESV_MRK_1-1-13_av360p00070.ts
#EXT-X-ENDLIST

view: bible_file_video_transport_stream
actual table: bible_file_stream_ts
unique key: file_name
+----------+---------------------+---------------------------------------+---------+---------------------+---------------------+
| id       | video_resolution_id | file_name                             | runtime | created_at          | updated_at          |
+----------+---------------------+---------------------------------------+---------+---------------------+---------------------+
| 14343752 |              210681 | English_ESV_MRK_1-1-13_av720p00000.ts |    3.08 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343753 |              210681 | English_ESV_MRK_1-1-13_av720p00001.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343754 |              210681 | English_ESV_MRK_1-1-13_av720p00002.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343755 |              210681 | English_ESV_MRK_1-1-13_av720p00003.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343756 |              210681 | English_ESV_MRK_1-1-13_av720p00004.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343757 |              210681 | English_ESV_MRK_1-1-13_av720p00005.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343758 |              210681 | English_ESV_MRK_1-1-13_av720p00006.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343759 |              210681 | English_ESV_MRK_1-1-13_av720p00007.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343760 |              210681 | English_ESV_MRK_1-1-13_av720p00008.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343761 |              210681 | English_ESV_MRK_1-1-13_av720p00009.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343762 |              210681 | English_ESV_MRK_1-1-13_av720p00010.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343763 |              210681 | English_ESV_MRK_1-1-13_av720p00011.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343764 |              210681 | English_ESV_MRK_1-1-13_av720p00012.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343765 |              210681 | English_ESV_MRK_1-1-13_av720p00013.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343766 |              210681 | English_ESV_MRK_1-1-13_av720p00014.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343767 |              210681 | English_ESV_MRK_1-1-13_av720p00015.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343768 |              210681 | English_ESV_MRK_1-1-13_av720p00016.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343769 |              210681 | English_ESV_MRK_1-1-13_av720p00017.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343770 |              210681 | English_ESV_MRK_1-1-13_av720p00018.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343771 |              210681 | English_ESV_MRK_1-1-13_av720p00019.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343772 |              210681 | English_ESV_MRK_1-1-13_av720p00020.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343773 |              210681 | English_ESV_MRK_1-1-13_av720p00021.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343774 |              210681 | English_ESV_MRK_1-1-13_av720p00022.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343775 |              210681 | English_ESV_MRK_1-1-13_av720p00023.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343776 |              210681 | English_ESV_MRK_1-1-13_av720p00024.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343777 |              210681 | English_ESV_MRK_1-1-13_av720p00025.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343778 |              210681 | English_ESV_MRK_1-1-13_av720p00026.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343779 |              210681 | English_ESV_MRK_1-1-13_av720p00027.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343780 |              210681 | English_ESV_MRK_1-1-13_av720p00028.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343781 |              210681 | English_ESV_MRK_1-1-13_av720p00029.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343782 |              210681 | English_ESV_MRK_1-1-13_av720p00030.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343783 |              210681 | English_ESV_MRK_1-1-13_av720p00031.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343784 |              210681 | English_ESV_MRK_1-1-13_av720p00032.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343785 |              210681 | English_ESV_MRK_1-1-13_av720p00033.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343786 |              210681 | English_ESV_MRK_1-1-13_av720p00034.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343787 |              210681 | English_ESV_MRK_1-1-13_av720p00035.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343788 |              210681 | English_ESV_MRK_1-1-13_av720p00036.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343789 |              210681 | English_ESV_MRK_1-1-13_av720p00037.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343790 |              210681 | English_ESV_MRK_1-1-13_av720p00038.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343791 |              210681 | English_ESV_MRK_1-1-13_av720p00039.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343792 |              210681 | English_ESV_MRK_1-1-13_av720p00040.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343793 |              210681 | English_ESV_MRK_1-1-13_av720p00041.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343794 |              210681 | English_ESV_MRK_1-1-13_av720p00042.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343795 |              210681 | English_ESV_MRK_1-1-13_av720p00043.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343796 |              210681 | English_ESV_MRK_1-1-13_av720p00044.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343797 |              210681 | English_ESV_MRK_1-1-13_av720p00045.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343798 |              210681 | English_ESV_MRK_1-1-13_av720p00046.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343799 |              210681 | English_ESV_MRK_1-1-13_av720p00047.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343800 |              210681 | English_ESV_MRK_1-1-13_av720p00048.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343801 |              210681 | English_ESV_MRK_1-1-13_av720p00049.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343802 |              210681 | English_ESV_MRK_1-1-13_av720p00050.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343803 |              210681 | English_ESV_MRK_1-1-13_av720p00051.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343804 |              210681 | English_ESV_MRK_1-1-13_av720p00052.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343805 |              210681 | English_ESV_MRK_1-1-13_av720p00053.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343806 |              210681 | English_ESV_MRK_1-1-13_av720p00054.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343807 |              210681 | English_ESV_MRK_1-1-13_av720p00055.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343808 |              210681 | English_ESV_MRK_1-1-13_av720p00056.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343809 |              210681 | English_ESV_MRK_1-1-13_av720p00057.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343810 |              210681 | English_ESV_MRK_1-1-13_av720p00058.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343811 |              210681 | English_ESV_MRK_1-1-13_av720p00059.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343812 |              210681 | English_ESV_MRK_1-1-13_av720p00060.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343813 |              210681 | English_ESV_MRK_1-1-13_av720p00061.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343814 |              210681 | English_ESV_MRK_1-1-13_av720p00062.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343815 |              210681 | English_ESV_MRK_1-1-13_av720p00063.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343816 |              210681 | English_ESV_MRK_1-1-13_av720p00064.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343817 |              210681 | English_ESV_MRK_1-1-13_av720p00065.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343818 |              210681 | English_ESV_MRK_1-1-13_av720p00066.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343819 |              210681 | English_ESV_MRK_1-1-13_av720p00067.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343820 |              210681 | English_ESV_MRK_1-1-13_av720p00068.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343821 |              210681 | English_ESV_MRK_1-1-13_av720p00069.ts |    3.00 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
| 14343822 |              210681 | English_ESV_MRK_1-1-13_av720p00070.ts |    1.46 | 2020-01-15 15:48:05 | 2020-01-15 15:48:05 |
+----------+---------------------+---------------------------------------+---------+---------------------+---------------------+
"""



