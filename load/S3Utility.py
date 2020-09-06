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
#import io
import os
import shutil
from Config import *

class S3Utility:

	def __init__(self, config):
		self.config = config
		#session = boto3.Session(profile_name='FCBH_Gary') # needs config
		session = boto3.Session(profile_name='Gary')
		#session = boto3.Session(profile_name='BibleApp')
		self.client = session.client('s3')


	def getObject(self, s3Bucket, s3Key):
		#try:
		response = self.client.get_object(Bucket=s3Bucket, Key=s3Key)
		print(response)


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


	## files start in the validate folder
	## When validation is completed on a fileset, that entire fileset is moved
	## by moving textCode/bibleId/filesetId to the upload folder

	## This stuff belongs in a main method
	## i.e. do validate one fileset at a time
	## move validated filesets to uploading
	## if a fileset exists in uploading and does not exist in validate, start upload
	## upload moves files one at a time to uploaded, and when complete, it moves
	## all files to uploaded.
	## the main program checks uploads, and that same fileset does not exists in uploading.
	## if so, it starts database update
	## when the database update is complete, it moves the entire fileset to complete


	def uploadAllFilesets(self, s3Bucket):
		directory = self.config.directory_uploading
		lenDirectory = len(directory)
		for root, dirs, files in os.walk(directory):
			relDirName = root[lenDirectory:].replace("\\", "/")
			if relDirName[:5] == "audio" and len(dirs) == 0:
				print("***** inside audio", directory, relDirName, dirs)
				self.uploadFileset(s3Bucket, directory, relDirName, files, "audio/mpeg")
			elif relDirName[:4] == "text" and relDirName.count("/") == 1:
				print("***** inside text", directory, relDirName, dirs)
			else:
				print("***** ELSEWHERE", directory, relDirName, dirs)


	def uploadFileset(self, s3Bucket, sourceDir, filesetPrefix, files, contentType):
		errorCount = 0
		targetDir = self.config.directory_uploaded
		os.makedirs(targetDir + filesetPrefix, exist_ok=True)
		for file in files:
			if not file.startswith("."):
				s3Key = filesetPrefix + os.sep + file
				filename = sourceDir + s3Key
				#try:
					#print("upload file", filename, "to key", s3Key)
					#self.client.upload_file(filename, s3Bucket, s3Key,
					#ExtraArgs={'ContentType': contentType})
				#except Exception as err:
					#print("ERROR: Upload of %s to %s failed: %s" % (s3Key, s3Bucket, err))
					#errorCount += 1
				try:
					moveFilename = targetDir + s3Key
					print("move file", filename, "to file", moveFilename)
					os.rename(filename, moveFilename)
				except Exception as err:
					print("ERROR: Rename of %s to %s failed: %s" % (filename, moveFilename, err))
					errorCount += 1

		if errorCount == 0:
			self.cleanupDirectory(sourceDir, filesetPrefix)
			self.promoteFileset(targetDir, filesetPrefix)


	def cleanupDirectory(self, directory, filesetPrefix):
		prefix = filesetPrefix
		print("prefix", prefix)
		while(len(prefix) > 0):
			path = directory + prefix
			print("path", path)
			files = os.listdir(path)
			print("files", len(files))
			if len(files) == 0:
				#os.remdir(path)
				print("remdir", path)
				pos = path.rfind(os.sep)
				print("pos", pos)
				if pos >= 0:
					prefix = prefix[:pos]
				else:
					prefix = ""
			else:
				prefix = ""
			print("new prefix")


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
		print("move dir", sourceDir + filesetPrefix, "to dir", targetDir)
		#shutil.move(sourceDir + filesetPrefix, targetDir)



	#def deleteFile(self, filename):
	#	os.remove(filename)

if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	s3.cleanupDirectory(config.directory_uploading, "audio/ENGESV/ENGESVN1DA")


	#s3.uploadAllFilesets("test-dbp")
	

	#s3.getObject("test-dbp", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3")
	#s3.getObject("test-dbp", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html")
	#s3.getObject("test-dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4")

	#s3.downloadFile("dbp-prod", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3", "audio.mp3")
	#s3.downloadFile("dbp-prod", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html", "text.html")
	#s3.downloadFile("dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4", "video.mp4")

	#s3.uploadFile("test-dbp", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3", "audio.mp3", "audio/mpeg")
	#s3.uploadFile("test-dbp", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html", "text.html", "text/html")
	#s3.uploadFile("test-dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4", "video.mp4", "video/mp4")


