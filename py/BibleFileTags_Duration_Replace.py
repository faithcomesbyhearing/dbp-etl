# BibleFileTags_Duration_Replace.py

# This program downloads audio files one at a time, computes the duration with ffprobe,
# and updates bible_file_tags with the correct value.
#
# 1) Does a query of bible_fileset_connections, bible_filesets, and bible_files
# so that it can obtain a complete key of all audio files.
# 2) There should be a way to limit it to a fileset for testing purposes
# 3) It should iterate over the records found, and download each file to a temp directory
# 4) It should use ffprob to compute the duration
# 5) It should generate replace statements from bible_file_tags
# 6) This program needs a restart capability that knows what has already been finished.


import sys
import os
from SQLUtility import *
import boto3
import subprocess


DUR_HOST = "localhost"
DUR_USER = "root"
DUR_PORT = 3306
DUR_DB_NAME = "dbp"
DUR_MP3_DIRECTORY = "%s/FCBH/files/tmp" % (os.environ["HOME"])
DUR_AWS_PROFILE = "FCBH_Gary"


class BibleFileTags_Duration_Replace:

	def __init__(self):
		self.db = SQLUtility(DUR_HOST, DUR_PORT, DUR_USER, DUR_DB_NAME)
		self.output = io.open("BibleFileTags_Duration_Replace.log", mode="a")


	def getCommandLine(self):
		if len(sys.argv) < 2:
			print("ERROR: Enter starting file_id, or 0")
			sys.exit()
		fileId = sys.argv[1]
		if not fileId.isdigit():
			print("ERROR: starting file_id must be integer")
			sys.exit()
		return fileId
		

	def getAudioKeys(self, startingFileId):
		sql = ("SELECT bfc.bible_id, bs.id, bf.file_name, bf.id"
			" FROM bible_fileset_connections bfc, bible_filesets bs, bible_files bf"
			" WHERE bfc.hash_id = bs.hash_id"
			" AND bs.hash_id = bf.hash_id"
			" AND bs.set_type_code in ('audio', 'audio_drama')"
			" AND bf.id >= %d"
			" ORDER BY bf.id")
		return self.db.select(sql, (startingFileId,))


	def getMP3File(self, s3Bucket, bibleId, filesetId, filename):
		s3Key = "audio/%s/%s/%s" % (bibleId, filesetId, filename)
		filepath = HLS_MP3_DIRECTORY + os.sep + s3Key
		if not os.access(filepath, os.R_OK):
			print("Must download %s" % (s3Key))
			directory = filepath[:filepath.rfind("/")]
			if not os.access(directory, os.R_OK):
				os.makedirs(directory)
			self.s3Client.download_file(s3Bucket, s3Key, filepath)
		return filepath


	## modify to return duration, experiment with this or find in lptsmanager uses mutagen
	def getDuration(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep bit_rate'
		response = subprocess.run(cmd, shell=True, capture_output=True)
		result = self.bitrateRegex.search(str(response))
		if result != None:
			return result.group(1)
		else:
			raise Exception("ffprobe for duration failed for %s" % (file))


	def updateDatabase(self, fileId, duration):
		sql = "REPLACE INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%d, 'duration', %d, 0)"
		self.db.execute(sql, (fileId, duration))
		self.output.write("%d\n" % (fileId))


	def process(self):
		startingFileId = self.getCommandLine()
		resultSet = self.getAudioKeys(startingFileId)
		for row in resultSet:
			bibleId = row[0]
			filesetId = row[1]
			filename = row[2]
			fileId = row[3]
			filepath = self.getMP3File("dbp-prod", bibleId, filesetId, filename)
			duration = self.getDuration(filepath)
			os.remove(filepath)
			self.updateDatabase(fileId, duration)
		self.db.close()
		self.output.close()



tags = BibleFileTags_Duration_Replace()
tags.process()


