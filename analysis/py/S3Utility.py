# S3Utility
#
# This file is a general purpose utility for performing S3 operations
#
import boto3
import io
import os 


class S3Utility:

	def __init__(self, config):
		self.config = config
		session = boto3.Session(profile_name='FCBH_Gary') # needs config
		self.client = session.client('s3')


	def downloadFile(self, s3Bucket, s3Key, filename):
		try:
			print("Download %s, %s to %s" % (s3Bucket, s3Key, filename))
			self.client.download_file(s3Bucket, s3Key, filename)
			print("Done With Download")
		except Exception as err:
			print("ERROR: Download %s failed with error %s" % (s3Key, err))


	def deleteFile(self, filename):
		a=1



