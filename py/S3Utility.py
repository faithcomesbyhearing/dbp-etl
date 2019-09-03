# S3Utility.py
#
# This class is convenience methods for uploading, downloading and comparing objects in AWS S3 buckets.
#

#import io
import os
import boto3
from Config import *

## The config buckets must not be changed in config.  These buckets must be used instead.
TEST_BUCKET = "test-dbp"
TEST_VID_BUCKET = "test-dbp-vid"
TEST_PROFILE = "Gary"
DEBUG = True # Set to false to use production buckets

class S3Utility:

	def __init__(self, config):
		if DEBUG:
			self.session = boto3.Session(profile_name = TEST_PROFILE)
		else:
			self.session = boto3.Session(profile_name = config.s3_aws_profile)
		self.client = self.session.client("s3")
		self.config = config
		self.messages = []


	def moveToValidate(self, sourceDir, bibleId, damId):
		return self.moveFiles(sourceDir, config.directory_validate, bibleId, damId)


	def moveToUpload(self, bibleId, damId):
		return self.moveFiles(config.directory_validate, config.directory_upload, bibleId, damId)


	def uploadAndMoveToDatabase(self, bibleId, damId):
		sourceDir = config.directory_upload
		targetDir = config.directory_database
		print(sourceDir, targetDir)
		self.ensureDirectory(targetDir, bibleId, damId)
		bucket = self.getBucket(damId)
		files = self.getPaths(sourceDir, bibleId, damId)
		count = 0
		for (path, s3Key) in files:
			sourcePath = os.path.join(sourceDir, path)
			targetPath = os.path.join(targetDir, path)
			print(sourcePath, targetPath, s3Key)
			try:
				response = self.client.upload_file(sourcePath, bucket, s3Key)
				os.rename(sourcePath, targetPath)
				count += 1
			except Exception as err:
				self.reportError("uploading S3 object", s3Key, err)
		return count


	def moveToComplete(self, bibleId, damId):
		return self.moveFiles(config.directory_database, config.directory_complete, bibleId, damId)


	def moveFiles(self, sourceDir, targetDir, bibleId, damId):
		self.ensureDirectory(targetDir, bibleId, damId)
		files = self.getPaths(sourceDir, bibleId, damId)
		count = 0
		for (path, s3Key) in files:
			sourcePath = os.path.join(sourceDir, path)
			targetPath = os.path.join(targetDir, path)
			try:
				os.rename(sourcePath, targetPath)
				count += 1
			except Exception as err:
				self.reportError("moving file", path, err)
		return count


	def compare(self, directory, bibleId, damId):
		bucket = self.getBucket(damId)
		files = self.getPaths(directory, bibleId, damId)
		count = 0
		for (path, s3Key) in files:
			sourcePath = os.path.join(directory, path)
			try:
				response = self.client.head_object(Bucket = bucket, Key = s3Key)
				objLength = response["ContentLength"]
				fileLength = os.path.getsize(sourcePath)
				if objLength != fileLength:
					msg = "S3 object %d bytes, File %d bytes" % (objLength, fileLength)
					self.reportError(msg, path, err)
				else:
					count += 1
			except Exception as err:
				self.reportError("comparing s3 object", s3Key, err)
		return count


	def getBucket(self, damId):
		if DEBUG:
			if damId[8:10] == "DV": ## if would be better if this was some centralized method
				return TEST_VID_BUCKET
			else:
				return TEST_BUCKET
		else:
			if damId[8:10] == "DV":
				return self.config.s3_vid_bucket
			else:
				return self.config.s3_bucket


	def ensureDirectory(self, rootDir, bibleId, damId):
		targetDir = os.path.join(rootDir, bibleId, damId)
		if not os.path.exists(targetDir):
			os.makedirs(targetDir)


	def getPaths(self, directory, bibleId, damId):
		print(directory, bibleId, damId)
		lenRoot = len(directory)
		paths = []
		rootDir = os.path.join(directory, bibleId, damId)
		print("rootDir", rootDir)
		for root, dirs, files in os.walk(rootDir):
			files = [f for f in files if f[0] != '.']
			dirs[:] = [d for d in dirs if d[0] != '.']
			for file in files:
				path = os.path.join(root[lenRoot:], file)
				if path[0] == os.sep:
					path = path[1:]
				s3Key = "/".join(path.split(os.sep))
				paths.append((path, s3Key))
		return paths


	def reportError(self, msg, obj, err):
		msg = "ERROR %s on %s -> %s" % (msg, obj, err)
		print(msg)
		self.messages.append(msg)




"""
## Unit Test
config = Config()
s3 = S3Utility(config)

#count = s3.moveToValidate("/Users/garygriswold/FCBH/files_staging", "ENGNIV", "ENGNIVP2DV")
#count = s3.moveToUpload("ENGNIV", "ENGNIVP2DV")
#count = s3.uploadAndMoveToDatabase("ENGNIV", "ENGNIVP2DV")
#count = s3.moveToComplete("ENGNIV", "ENGNIVP2DV")
count = s3.compare(config.directory_complete, "ENGNIV", "ENGNIVP2DV")
print(count)
"""







