# UpdateDBPVideoTables.py

# This program updates the bible_file_stream_bandwidth, and bible_file_stream_ts files for video

import sys
import os
import re
import subprocess
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from TranscodeVideo import *
from S3Utility import *
from UpdateDBPFilesetTables import *


class UpdateDBPVideoTables:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.s3Utility = S3Utility(config)
		sql = ("SELECT file_name, id FROM bible_files where hash_id in"
			" (SELECT hash_id FROM bible_filesets WHERE set_type_code = 'video_stream')")
		self.fileIdMap = db.selectMap(sql, ())
		self.streamRegex = re.compile(r"BANDWIDTH=([0-9]+),RESOLUTION=([0-9]+)x([0-9]+),CODECS=\"(.+)\"")
		self.tsRegex = re.compile(r"#EXTINF:([0-9\.]+),")
		self.dbpBandwidthMap = None
		self.dbpTsFileMap = None
		self.dbpBandwidthIdMap = None
		self.dbpBandwidthInsertUpdateSet = set()
		self.dbpTsInsertUpdateSet = set()


	def processFileset(self, filesetPrefix, hashId):
		if hashId != None:
			self.populateDBPMaps(hashId)
			done = self.updateVideoFileset(self.config.directory_database, filesetPrefix)
			if done:
				print("Update video %s succeeded." % (filesetPrefix))
			else:
				print("Update video %s FAILED." % (filesetPrefix))
			self.computeDurations(hashId)
			self.processDeletions()


	def populateDBPMaps(self, hashId):
		sql = ("SELECT bible_file_id, file_name, bandwidth, resolution_width, resolution_height, codec, stream"
			" FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id = %s)")
		resultSet = self.db.select(sql, (hashId,))
		self.dbpBandwidthMap = {}
		for (bibleFileId, fileName, bandwidth, resolutionWidth, resolutionHeight, codec, stream) in resultSet:
			self.dbpBandwidthMap[fileName] = (bibleFileId, bandwidth, resolutionWidth, resolutionHeight, codec, stream)

		sql = ("SELECT stream_bandwidth_id, file_name, runtime FROM bible_file_stream_ts WHERE stream_bandwidth_id IN"
			" (SELECT id FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id = %s))")
		resultSet = self.db.select(sql, (hashId,))
		self.dbpTsFileMap = {}
		for (streamBandwidthId, fileName, runtime) in resultSet:
			self.dbpTsFileMap[fileName] = (streamBandwidthId, runtime)

		sql = ("SELECT file_name, id FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id = %s)")
		self.dbpBandwidthIdMap = self.db.selectMap(sql, (hashId,))
		self.dbpBandwidthInsertUpdateSet = set()
		self.dbpTsInsertUpdateSet = set()


	def updateVideoFileset(self, directory, filesetPrefix):
		errorCount = 0
		for filename in [f for f in os.listdir(directory + filesetPrefix) if not f.startswith('.')]:
			m3u8Files = self.downloadM3U8(filesetPrefix, filename)
			for (m3u8Filename, m3u8Content) in m3u8Files.items():
				if m3u8Filename.endswith("_stream.m3u8"):
					self.processStreamM3U8(m3u8Filename, m3u8Content)
				else:
					self.processTSM3U8(m3u8Filename, m3u8Content)
		return errorCount == 0


	def downloadM3U8(self, filesetPrefix, filename):
		m3u8Files = {}
		suffixes = TranscodeVideo.getHLSTypes()
		name = filename.split(".")[0]
		for suffix in suffixes:
			m3u8Filename =  name + suffix + ".m3u8"
			s3Key = filesetPrefix + m3u8Filename
			content = self.s3Utility.getAsciiObject(self.config.s3_vid_bucket, s3Key)
			m3u8Files[m3u8Filename] = content
		return m3u8Files


	def processStreamM3U8(self, m3u8Filename, m3u8Content):
		insertRows = []
		updateRows = []
		deleteRows = []

		fileId = self.fileIdMap.get(m3u8Filename)
		if fileId == None:
			fileId = "@" + m3u8Filename.replace("-", "_")
		for line in m3u8Content.split("\n"):
			if line.startswith("#EXT-X-STREAM-INF:"):
				match = self.streamRegex.search(line)
				bandwidth = int(match.group(1))
				width = int(match.group(2))
				height = int(match.group(3))
				codec = match.group(4)
				stream = 1
			elif line.endswith("m3u8"):
				filename = line
				self.dbpBandwidthInsertUpdateSet.add(filename)

				if filename not in self.dbpBandwidthMap.keys():
					insertRows.append((fileId, bandwidth, width, height, codec, stream, filename))
				else:
					(dbpFileId, dbpBandwidth, dbpWidth, dbpHeight, dbpCodec, dbpStream) = self.dbpBandwidthMap[filename]
					if isinstance(fileId, int) and fileId != dbpFileId:
						updateRows.append(("bible_file_id", fileId, dbpFileId, filename))
					if bandwidth != dbpBandwidth:
						updateRows.append(("bandwidth", bandwidth, dbpBandwidth, filename))
					if width != dbpWidth:
						updateRows.append(("resolution_width", width, dbpWidth, filename))
					if height != dbpHeight:
						updateRows.append(("resolution_height", height, dbpHeight, filename))
					if codec != dbpCodec:
						updateRows.append(("codec", codec, dbpCodec, filename))
					if stream != dbpStream:
						updateRows.append(("stream", stream, dbpStream, filename))

		tableName = "bible_file_stream_bandwidths"
		pkeyNames = ("file_name",)
		attrNames = ("bible_file_id", "bandwidth", "resolution_width", "resolution_height", "codec", "stream")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows, 6)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)


	def processTSM3U8(self, m3u8Filename, m3u8Content):
		insertRows = []
		updateRows = []
		deleteRows = []

		bandwidthId = self.dbpBandwidthIdMap.get(m3u8Filename)
		if bandwidthId == None:
			bandwidthId = "@" + m3u8Filename.replace("-", "_")
		else:
			bandwidthId = int(bandwidthId)
		for line in m3u8Content.split("\n"):
			if line.startswith("#EXTINF:"):
				match = self.tsRegex.match(line)
				runtime = round(float(match.group(1)), 2)
			elif line.endswith("ts"):
				filename = line
				self.dbpTsInsertUpdateSet.add(filename)

				if filename not in self.dbpTsFileMap.keys():
					insertRows.append((bandwidthId, runtime, filename))
				else:
					(dbpBandwidthId, dbpRuntime) = self.dbpTsFileMap[filename]
					if isinstance(bandwidthId, int) and bandwidthId != dbpBandwidthId:
						updateRows.append(("video_resolution_id", bandwidthId, dbpBandwidthId, filename))
					if runtime != dbpRuntime:
						updateRows.append(("runtime", runtime, dbpRuntime, filename))

		tableName = "bible_file_stream_ts"
		pkeyNames = ("file_name",)
		attrNames = ("stream_bandwidth_id", "runtime")
		self.dbOut.insertSet(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)


	def computeDurations(self, hashId):
		sql = ("UPDATE bible_files bf INNER JOIN"
			" (SELECT DISTINCT bf.id AS ID, SUM(bfvts.runtime) AS Duration"
			" FROM bible_files bf"
			" JOIN bible_filesets bfs ON bfs.hash_id=bf.hash_id"
			" JOIN bible_file_stream_bandwidths bfvr ON bfvr.bible_file_id=bf.id"
			" JOIN bible_file_stream_ts bfvts ON bfvts.stream_bandwidth_id=bfvr.id"
			" WHERE bf.id IN (SELECT bf.id FROM bible_files bf"
			" WHERE bf.hash_id='%s' AND bf.duration IS NULL)"
			" GROUP BY bf.id, bfvr.id) bfu"
			" ON bf.id=bfu.ID SET bf.duration=bfu.Duration;")
		self.dbOut.rawStatement(sql % (hashId,))


	def processDeletions(self):
		deleteRows = []
		for dbpFilename in self.dbpTsFileMap.keys():
			if dbpFilename not in self.dbpTsInsertUpdateSet:
				deleteRows.append((dbpFilename,))
		tableName = "bible_file_stream_ts"
		pkeyNames = ("file_name",)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)

		deleteRows = []		
		for dbpFilename in self.dbpBandwidthMap.keys():
			if dbpFilename not in self.dbpBandwidthInsertUpdateSet:
				deleteRows.append((dbpFilename,))
		tableName = "bible_file_stream_bandwidths"
		pkeyNames = ("file_name",)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	## This is here for development debugging only
	def deleteVideoFiles(db, filesetId, deleteFileset=False):
		sql = []
		sql.append("DELETE bfss FROM bible_file_stream_ts AS bfss"
			" JOIN bible_file_stream_bandwidths AS bfsb ON bfss.stream_bandwidth_id = bfsb.id"
			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE bfsb FROM bible_file_stream_bandwidths AS bfsb"
			" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE bft FROM bible_file_tags AS bft"
			" JOIN bible_files AS bf ON bft.file_id = bf.id"
			" WHERE bf.hash_id = %s")
		sql.append("DELETE FROM bible_files WHERE hash_id = %s")
		if deleteFileset:
			sql.append("DELETE FROM access_group_filesets WHERE hash_id = %s")
			sql.append("DELETE FROM bible_fileset_connections WHERE hash_id = %s")
			sql.append("DELETE FROM bible_fileset_tags WHERE hash_id = %s")
			sql.append("DELETE FROM bible_filesets WHERE hash_id = %s")
		hashId = db.selectScalar("SELECT hash_id FROM bible_filesets WHERE id=%s", (filesetId,)) ## This won't work
		for stmt in sql:
			db.execute(stmt, (hashId,))


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	#UpdateDBPVideoTables.deleteVideoFiles(db, "ENGESVP2DV", True)
	#sys.exit()
	dbOut = SQLBatchExec(config)
	update = UpdateDBPFilesetTables(config, db, dbOut)
	completed = update.process()
	video = UpdateDBPVideoTables(config, db, dbOut)
	video.process(completed)
	dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute("video")





