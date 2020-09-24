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
import boto3.s3
import boto3.s3.transfer
import os
import shutil
#from boto.s3.connection import S3Connection  # test
#from multiprocessing.pool import ThreadPool  # test
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


	def uploadAllFilesets(self):
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


	def uploadAudioFileset(self, s3Bucket, sourceDir, filesetPrefix, files):
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
		self._cleanupDirectory(sourceDir, filesetPrefix)


	def _cleanupDirectory(self, directory, filesetPrefix):
		print("cleanup directory %s/%s" % (directory, filesetPrefix))
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
	session = boto3.Session(profile_name=config.s3_aws_profile)
	client = session.client('s3')
	transConfig = boto3.s3.transfer.TransferConfig()
	transfer = boto3.s3.transfer.S3Transfer(client=client, config=transConfig, osutil=None, manager=None)
	directory = config.directory_uploading
	for typeCode in os.listdir(directory):
		if typeCode == "audio":
			for bibleId in os.listdir(directory + "/audio"):
				for filesetId in os.listdir(directory + "/audio/" + bibleId):
					print(filesetId)
					for file in os.listdir(directory + "/audio/" + bibleId + "/" + filesetId):
						print(file)
						key = typeCode + "/" + bibleId + "/" + filesetId + "/" + file
						filename = directory + key
						#print(key, filename)
						transfer.upload_file(filename, "test-dbp", key, callback=None, extra_args=None)

"""
if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	s3.uploadAllFilesets()
"""
"""
time aws s3 sync /Volumes/FCBH/files/uploading/audio/ENGESV/ENGESVN2DA/ s3://test-dbp/audio/ENGESV/ENGESVN2DA/

#time aws s3 rm 

time aws s3 cp /Volumes/FCBH/files/uploading/audio/ENGESV/ENGESVN2DA/ s3://test-dbp/audio/ENGESV/ENGESVN2DA/ --recursive


		print("AUDIO folder exists")
		subfolders=folderList(sourcePath+"audio/")
		for folder in subfolders:
			subGroup=folderList(sourcePath+"audio/"+folder+"/")
			for group in subGroup:
				fileGroup=listFiles(sourcePath+"audio/"+folder+"/"+group+"/")
				for audioFile in fileGroup:
					audioPath=sourcePath+"audio/"+folder+"/"+group+"/"+audioFile
					duration=getDurationMP3(audioPath)
					if duration is not None:
						audioFileDic.update({audioFile:duration})
				cmdRM="aws s3 rm s3://"+lptsEnv.bucket+"/audio/"+folder+"/"+group+"/ --recursive"
				print(cmdRM)
				os.system(cmdRM)
				
				cmd="aws s3 cp "+sourcePath+"audio/"+folder+"/"+group+"/ s3://"+lptsEnv.bucket+"/audio/"+folder+"/"+group+"/ --recursive"
				print(cmd)
				os.system(cmd)
				x = datetime.datetime.now()
				thingsToCheck.add(x.strftime("%Y-%m-%d %H:%M:%S")+" "+folder+":"+group)
			os.system("rm -r "+sourcePath+"audio/"+folder)
	
"""
