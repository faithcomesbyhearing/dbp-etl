# BibleFiles_Duration_Update.py

# This program downloads audio files one at a time, computes the duration with ffprobe,
# and updates bible_file_tags with the correct value.
#
# Usage: python3 py/BibleFiles_Duration_Update.py start_bible_id ending_bible_id
#
# This program processes files in bible_id, fileset_id, filename sequence, 
# and requires a starting bible_id and ending bible_id as command line parameters.
# So, a stopped process can be restarted at any point.
#
# This program produces a log that contains a list of bible_id's processed.
# This is done to provide a restart capability.
#
# 1) Does a query of bible_fileset_connections, bible_filesets, and bible_files
# so that it can obtain a complete key of all audio files.
# 3) It should iterate over the records found, and download each file to a temp directory
# 4) It should use ffprob to compute the duration
# 5) It should generate replace statements from bible_file_tags


import sys
import os
import io
import re
import math
from SQLUtility import *
import boto3
import subprocess


DUR_HOST = "localhost"
DUR_USER = "root"
DUR_PORT = 3306
DUR_DB_NAME = "dbp"
DUR_MP3_DIRECTORY = "%s/FCBH/files/tmp" % (os.environ["HOME"])
DUR_AWS_PROFILE = "FCBH_Gary"


class BibleFiles_Duration_Update:

	def __init__(self):
		self.db = SQLUtility(DUR_HOST, DUR_PORT, DUR_USER, DUR_DB_NAME)
		session = boto3.Session(profile_name=DUR_AWS_PROFILE)
		self.s3Client = session.client("s3")
		self.durationRegex = re.compile(r"duration=([0-9\.]+)")
		self.output = io.open("BibleFiles_Duration_Update.log", mode="w")


	def getCommandLine(self):
		if len(sys.argv) < 3:
			print("Usage: BibleFiles_Duration_Update.py starting_bible_id ending_bible_id")
			sys.exit()
		return(sys.argv[1], sys.argv[2])
		

	def getAudioKeys(self, startingBibleId, endingBibleId):
		sql = ("SELECT bfc.bible_id, bs.id, bf.file_name, bf.id"
			" FROM bible_fileset_connections bfc, bible_filesets bs, bible_files bf"
			" WHERE bfc.hash_id = bs.hash_id"
			" AND bs.hash_id = bf.hash_id"
			" AND bs.set_type_code in ('audio', 'audio_drama')"
			" AND bfc.bible_id >= %s AND bfc.bible_id <= %s"
			" ORDER BY bfc.bible_id, bs.id, bf.file_name")
		return self.db.select(sql, (startingBibleId, endingBibleId))

#SELECT bfc.bible_id, bs.id, bf.file_name, bf.id, bf.duration, bf.file_size
#FROM bible_fileset_connections bfc, bible_filesets bs, bible_files bf
#WHERE bfc.hash_id = bs.hash_id
#AND bs.hash_id = bf.hash_id
#AND bs.set_type_code in ('audio', 'audio_drama')
#AND bfc.bible_id >= 'MPXPNG' AND bfc.bible_id <= 'MPXPNG'
#ORDER BY bfc.bible_id, bs.id, bf.file_name


	def getMP3File(self, s3Bucket, bibleId, filesetId, filename):
		s3Key = "audio/%s/%s/%s" % (bibleId, filesetId, filename)
		filepath = DUR_MP3_DIRECTORY + os.sep + s3Key.replace("/", ":")
		if not os.access(filepath, os.R_OK):
			#print("Download %s" % (s3Key))
			directory = filepath[:filepath.rfind("/")]
			if not os.access(directory, os.R_OK):
				os.makedirs(directory)
			self.s3Client.download_file(s3Bucket, s3Key, filepath)
		return filepath


	def getDuration(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep duration'
		response = subprocess.run(cmd, shell=True, capture_output=True)
		result = self.durationRegex.search(str(response))
		if result != None:
			dur = result.group(1)
			#print("duration", dur)
			return int(math.ceil(float(dur)))
		else:
			raise Exception("ffprobe for duration failed for %s" % (file))


	def getFileSize(self, file):
		stat = os.stat(file)
		#print("stat", stat)
		return stat.st_size


	def updateDatabase(self, fileId, duration, fileSize):
		#sql = "REPLACE INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%s, 'duration', %s, 0)"
		sql = "UPDATE bible_files SET duration = %s, file_size = %s WHERE id = %s"
		self.db.execute(sql, (duration, fileSize, fileId))


	def process(self):
		(startingBibleIdId, endingBibleId) = self.getCommandLine()
		resultSet = self.getAudioKeys(startingBibleIdId, endingBibleId)
		print("resultSet", len(resultSet))
		priorBibleId = None
		for (bibleId, filesetId, filename, fileId) in resultSet:
			print("audio", bibleId, filesetId, filename, fileId)
			if priorBibleId != bibleId:
				if priorBibleId != None:
					self.output.write("%s\n" % (priorBibleId))
				priorBibleId = bibleId
			filepath = self.getMP3File("dbp-prod", bibleId, filesetId, filename)
			duration = self.getDuration(filepath)
			#print("duration", duration)
			fileSize = self.getFileSize(filepath)
			#print("filesize", fileSize)
			os.remove(filepath)
			self.updateDatabase(fileId, duration, fileSize)
		self.db.close()
		self.output.write("%s\n" % (priorBibleId))
		self.output.close()



tags = BibleFiles_Duration_Update()
tags.process()




