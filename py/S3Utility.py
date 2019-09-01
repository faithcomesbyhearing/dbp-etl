# S3Utility.py
#
# This class is convenience methods for uploading, downloading and comparing objects in AWS S3 buckets.
#
# I can't really finish this until I know whether an asset file is stored in a hierarchy of directories
# such as audio/bibleId/damId... or in a single directory like audio:bibleId:damId

import io
import os
import boto3
from Config import *

class S3Utility:

	def __init__(self, config):
		self.session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = self.session.client("s3")
		self.config = config


	def upload(self, s3Key):
		bucket = self.getBucket(s3Key)
		sourcePath = os.path.join(self.config.directory_upload, s3Key)
		print sourcePath
		try:
			#self.client.upload_file(bucket, s3Key, sourcePath)
			#targetPath = os.path(self.config.directory_database, s3Key)
			#os.rename(sourcePath, targetPath)
			return True
		except Exception, err:
			print("ERROR uploading s3 object '%s' on %s" % (err, s3Key))
			return False


	def download(self, s3Key, dirPath):
		bucket = self.getBucket(s3Key)
		sourcePath = os.path.join(dirPath, s3Key)
		try:
			self.client.download_file(bucket, s3Key, filename)
			return True
		except Exception, err:
			print("ERROR downloading s3 object '%s' on %s" % (err, s3Key))
			return False


	def compare(self, s3Key, dirPath):
		bucket = self.getBucket(s3Key)
		sourcePath = os.path.join(dirPath, s3Key)
		# get size of source file
		try:
			hashMap = self.client.head(bucket, s3Key)
			# get size of object
			# if the sizes are not equal produce an error
		except Exception, err:
			print("ERROR comparing s3 object '%s' on %s" % (err, s3Key))
			return False


	def getBucket(self, s3Key):
		# This function is not correct
		if s3Key.count(".ts")>0:
			return self.config.s3_vid_bucket
		else:
			return self.config.s3_bucket


## Unit Test
config = Config()
s3 = S3Utility(config)
result = s3.upload("audio/ACFWBT/DOMBEC/info.json")
print result





