# S3Utility
#
# This file is a general purpose utility for performing S3 operations
#
#
# Folder: audio
#	Folder: BibleID
#		Folder: FilesetID
#			Files: .mp3 files
#
# Folder: Booknames
#	Folder: BibleID
#		File: Booknames.xml
# Folder: Sql
#	Folder: BibleID
#		Folder: FilesetID
#			File: Sql file for plain text
# Folder: text
#	Folder: BibleID
#		Folder: filesetID
#			Folder: FONTs
#			Folder: index
#			Folder: indexlemma
#			Files: html txt files for formatted text
#		File: about.html
#		File: index.html
#		File: title.json
#
# Folder: video
#	Folder: BibleID
#		Folder: FilesetID
#			Files: .mp4 files
#

import boto3
import os
import shutil
from Config import *

class S3Utility:

	def __init__(self, config):
		self.config = config
		#session = boto3.Session(profile_name='Gary')
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')





	def downloadFile(self, s3Bucket, s3Key, filename):
		try:
			print("Download %s, %s to %s" % (s3Bucket, s3Key, filename))
			self.client.download_file(s3Bucket, s3Key, filename)
			print("Done With Download")
		except Exception as err:
			print("ERROR: Download %s failed with error %s" % (s3Key, err))


	def uploadFile(self, s3Bucket, s3Key, filename, contentType):
		try:
			self.client.upload_file(filename, s3Bucket, s3Key,
				ExtraArgs={'ContentType': contentType})
		except Exception as err:
			print("ERROR: Upload %s failed  with error %s" % (s3Key, err))


	def uploadAllFilesets(self, s3Bucket):
		directory = self.config.directory_uploading
		lenDirectory = len(directory)
		for root, dirs, files in os.walk(directory):
			relDirName = root[lenDirectory:].replace("\\", "/")
			if relDirName[:5] == "audio" and len(dirs) == 0:
				print("***** inside audio", directory, relDirName, dirs)
				self.uploadFileset(s3Bucket, directory, relDirName, files, "audio/mpeg")
			#elif relDirName[:4] == "text" and relDirName.count("/") == 1:
			#	print("***** inside text", directory, relDirName, dirs)
			#else:
			#	print("***** ELSEWHERE", directory, relDirName, dirs)


	def uploadFileset(self, s3Bucket, sourceDir, filesetPrefix, files, contentType):
		errorCount = 0
		targetDir = self.config.directory_uploaded
		os.makedirs(targetDir + filesetPrefix, exist_ok=True)
		for file in sorted(files):
			if not file.startswith("."):
				s3Key = filesetPrefix + os.sep + file
				filename = sourceDir + s3Key
				try:
					print("upload: %s" % (s3Key))
					self.client.upload_file(filename, s3Bucket, s3Key,
					ExtraArgs={'ContentType': contentType})
					moveFilename = targetDir + s3Key
					os.rename(filename, moveFilename)
				except Exception as err:
					print("ERROR: Upload of %s to %s failed: %s" % (s3Key, s3Bucket, err))
					errorCount += 1

		if errorCount == 0:
			self.cleanupDirectory(sourceDir, filesetPrefix)
			self.promoteFileset(targetDir, filesetPrefix)
			pos = filesetPrefix.rfind(os.sep)
			if pos >= 0:
				prefix = filesetPrefix[:pos]
				self.cleanupDirectory(targetDir, prefix)


	def cleanupDirectory(self, directory, filesetPrefix):
		prefix = filesetPrefix
		while(len(prefix) > 0):
			path = directory + prefix
			files = os.listdir(path)
			if len(files) == 0:
				try:
					os.rmdir(path)
				except Exception as err:
					print("ERROR: Directory cleanup of %s failed:" % (prefix, err))
				pos = prefix.rfind(os.sep)
				if pos >= 0:
					prefix = prefix[:pos]
				else:
					prefix = ""
			else:
				prefix = ""


	def promoteFileset(self, sourceDir, filesetPrefix):
		folders = [self.config.directory_validate,
				self.config.directory_uploading,
				self.config.directory_uploaded,
				self.config.directory_database,
				self.config.directory_complete]
		if not sourceDir in folders:
			print("FATAL: unknown directory %s in promote fileset" % (sourceDir))
			sys.exit()
		index = folders.index(sourceDir)
		nextIndex = index + 1
		if nextIndex >= len(folders):
			print("FATAL: cannot promote beyond directory %s" % (sourceDir))
			sys.exit()
		targetDir = folders[nextIndex]
		#print("move dir", sourceDir + filesetPrefix, "to dir", targetDir)
		try:
			shutil.move(sourceDir + filesetPrefix, targetDir + filesetPrefix)
		except Exception as err:
			print("ERROR: Directory move of %s failed:" % (filesetPrefix, err))


if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	s3.uploadAllFilesets(config.s3_bucket)
	

