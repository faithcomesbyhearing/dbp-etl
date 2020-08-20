# S3Utility
#
# This file is a general purpose utility for performing S3 operations
#
import boto3
import io
import os 
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


	def promoteFileset(self, filesetPrefix):
		folders = [self.config.directory_validate,
				self.directory_uploading,
				self.directory_uploaded,
				self.directory_database,
				self.directory_complete]
		# Does something keep track of the status's
		# Or, can I make this event driven, where the program responds to the presence
		# of files in a directory
		print("tbd")


	def uploadAllFilesets(self, s3Bucket):
		directory = self.config.directory_uploading
		lenDirectory = len(directory)
		for root, dirs, files in os.walk(directory):
			relDirName = root[lenDirectory:].replace("\\", "/")
			print(root, relDirName, dirs)
			#if len(files) > 0:
			#	print("file0", files[0])

			if relDirName[:5] == "audio" and len(dirs) == 0:
				print("***** inside audio", root, relDirName)
				self.uploadFileset(s3Bucket, root, relDirName, files, "audio/mpeg")
			elif relDirName[:4] == "text" and relDirName.count("/") == 1:
				print("***** inside text")
			else:
				print("***** ELSEWHERE")


	def uploadFileset(self, s3Bucket, sourceDir, filesetPrefix, files, contentType):
		errorCount = 0
		targetDir = self.config.directory_uploaded
		for file in files:
			filename = sourceDir + file
			s3Key = filesetPrefix + file
			moveFilename = targetDir + filesetPrefix + file
			print("upload file", filename, "to key", s3Key)
			print("move file", filename, "to file", moveFilename)
			#try:
				#self.client.upload_file(filename, s3Bucket, s3Key,
				#ExtraArgs={'ContentType': contentType})
				#os.mv(filename, moveFilename)
			#except Exception as err:
			#	print("ERROR: Upload of %s to %s failed: %s" % (s3Key, s3Bucket, err))
			#	errorCount += 1
		if errorCount == 0:
			sourceDir = targetDir + filesetPrefix
			targetDir = self.config.directory_database + filesetPrefix
			print("move dir", sourceDir, "to dir", targetDir)
			#os.mv(sourceDir, targetDir)



	#def deleteFile(self, filename):
	#	os.remove(filename)

if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	s3.uploadAllFilesets("test-dbp")
	#s3.uploadFileset("dbp-prod", "abcdefg")

	#s3.getObject("test-dbp", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3")
	#s3.getObject("test-dbp", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html")
	#s3.getObject("test-dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4")

	#s3.downloadFile("dbp-prod", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3", "audio.mp3")
	#s3.downloadFile("dbp-prod", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html", "text.html")
	#s3.downloadFile("dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4", "video.mp4")

	#s3.uploadFile("test-dbp", "audio/ACMAS3/ACMAS3N2DA16/B07___09_1Corinthians__ACMAS3N2DA.mp3", "audio.mp3", "audio/mpeg")
	#s3.uploadFile("test-dbp", "text/DIDWBT/DIDWBT/DIDWBT_73_JHN_3.html", "text.html", "text/html")
	#s3.uploadFile("test-dbp-vid", "video/ABIWBT/ABIWBTP2DV/Abidji_MRK_10-1-16.mp4", "video.mp4", "video/mp4")


