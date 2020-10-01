# S3Utility
#
# This file performs the S3Upload operations
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
import subprocess
from Config import *

class S3Utility:

	def __init__(self, config):
		self.config = config
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

	## deprecated
	def uploadAllFilesetsOld(self):
		directory = self.config.directory_uploading
		lenDirectory = len(directory)
		for root, dirs, files in os.walk(directory):
			relDirName = root[lenDirectory:].replace("\\", "/")
			if relDirName[:5] == "audio" and len(dirs) == 0:
				#print("***** inside audio", directory, relDirName, dirs)
				self.uploadAudioFileset(self.config.s3_bucket, directory, relDirName, files)
			#elif relDirName[:4] == "text" and relDirName.count("/") == 1:
			#	print("***** inside text", directory, relDirName, dirs)
			#else:
			#	print("***** ELSEWHERE", directory, relDirName, dirs)

	## deprecated
	def uploadAudioFilesetOld(self, s3Bucket, sourceDir, filesetPrefix, files):
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
					ExtraArgs={'ContentType': "audio/mpeg"})
					moveFilename = targetDir + s3Key
					os.rename(filename, moveFilename)
				except Exception as err:
					print("ERROR: Upload of %s to %s failed: %s" % (s3Key, s3Bucket, err))
					errorCount += 1

		if errorCount == 0:
			self._cleanupDirectory(sourceDir, filesetPrefix)
			self.promoteFileset(targetDir, filesetPrefix)


	def uploadAllFilesets(self):
		uploadSucceeded = 0
		uploadFailed = 0
		directory = self.config.directory_upload
		for typeCode in os.listdir(directory):
			if typeCode in {"audio", "text", "video"}:
				for bibleId in [f for f in os.listdir(directory + typeCode) if not f.startswith('.')]:
					for filesetId in [f for f in os.listdir(directory + typeCode + os.sep + bibleId) if not f.startswith('.')]:
						if typeCode == "audio":
							filesetPrefix = typeCode + "/" + bibleId + "/" + filesetId + "/"
							done = self.uploadAudioFileset(self.config.s3_bucket, directory, filesetPrefix)
							if done:
								uploadSucceeded += 1
							else:
								uploadFailed += 1
		print("%d filesets uploaded" % (uploadSucceeded))
		print("%d filesets failed to upload" % (uploadFailed))


	def uploadAudioFileset(self, s3Bucket, sourceDir, filesetPrefix):
		print("uploading: ", filesetPrefix)
		self._cleanupHiddenFiles(sourceDir + filesetPrefix)
		cmd = "aws s3 sync %s%s s3://%s/%s" % (sourceDir, filesetPrefix, s3Bucket, filesetPrefix)
		#cmd = 'aws s3 cp %s%s s3://%s/%s --recursive --exclude ".*" --exclude "*/.*"' % (sourceDir, filesetPrefix, s3Bucket, filesetPrefix)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0:
			print("ERROR: Upload of %s to %s failed. MESSAGE: %s" % (filesetPrefix, s3Bucket, response.stderr))
			return False
		else:
			self.promoteFileset(sourceDir, filesetPrefix)
			return True


	## Possibly this should be moved to be before Validate in class DBPLoadController
	def _cleanupHiddenFiles(self, directory):
		toDelete = []
		self._cleanupHiddenFilesRecurse(toDelete, directory)
		for path in toDelete:
			os.remove(path)


	def _cleanupHiddenFilesRecurse(self, toDelete, directory):
		for pathName in os.listdir(directory):
			fullName = directory + os.sep + pathName
			if pathName.startswith(".") or pathName == "Thumbs.db":
				toDelete.append(fullName)
			if os.path.isdir(fullName):
				self._cleanupHiddenFilesRecurse(toDelete, fullName)


	def promoteFileset(self, sourceDir, filesetPrefix):
		folders = [self.config.directory_validate,
				self.config.directory_upload,
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
			print("ERROR: Directory move of %s failed: %s" % (filesetPrefix, err))
		self._cleanupDirectory(sourceDir, filesetPrefix)


	def _cleanupDirectory(self, directory, filesetPrefix):
		prefix = filesetPrefix
		while(len(prefix) > 0):
			path = directory + prefix
			if os.path.exists(path):
				files = os.listdir(path)
				if len(files) == 0:
					try:
						os.rmdir(path)
					except Exception as err:
						print("ERROR: Directory cleanup of %s failed:" % (prefix, err))
					pos = prefix.rfind(os.sep)
					prefix = prefix[:pos] if pos > 0 else ""
				else:
					prefix = ""
			else:
				pos = prefix.rfind(os.sep)
				prefix = prefix[:pos] if pos > 0 else ""


if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	s3.uploadAllFilesets()


