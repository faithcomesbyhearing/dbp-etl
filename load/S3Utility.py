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
from TranscodeVideo import *

class S3Utility:

	def __init__(self, config):
		self.config = config
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')


	def getAsciiObject(self, s3Bucket, s3Key):
		obj = self.client.get_object(Bucket=s3Bucket, Key=s3Key)
		content = obj['Body'].read().decode('ascii')
		return content


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


	def uploadAllFilesets(self, filesets):
		for (typeCode, bibleId, filesetId, filesetPrefix, csvFilename) in filesets:
			s3Bucket = self.config.s3_vid_bucket if typeCode == "video" else self.config.s3_bucket
			if typeCode in {"audio", "video"}:
				done = self.uploadFileset(s3Bucket, self.config.directory_upload, filesetPrefix)
				if done:
					print("Upload %s succeeded." % (filesetPrefix,))
				else:
					print("Upload %s FAILED." % (filesetPrefix,))
							
			elif typeCode == "text":
				self.promoteFileset(self.config.directory_upload, filesetPrefix)


	def uploadFileset(self, s3Bucket, sourceDir, filesetPrefix):
		print("uploading: ", filesetPrefix)
		if self.config.s3_aws_profile == None:
			cmd = "aws s3 sync %s%s s3://%s/%s" % (sourceDir, filesetPrefix, s3Bucket, filesetPrefix)
		else:
			cmd = "aws --profile %s s3 sync %s%s s3://%s/%s" % (self.config.s3_aws_profile, sourceDir, filesetPrefix, s3Bucket, filesetPrefix)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0:
			print("ERROR: Upload of %s to %s failed. MESSAGE: %s" % (filesetPrefix, s3Bucket, response.stderr))
			return False
		else:
			if filesetPrefix.startswith("video"):
				TranscodeVideo.transcodeVideoFileset(self.config, filesetPrefix)
			self.promoteFileset(sourceDir, filesetPrefix)
			return True


	def promoteFileset(self, sourceDir, filesetPrefix):
		folders = [self.config.directory_upload,
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
		while(len(prefix) > 10):
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


