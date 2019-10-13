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

"""
config = Config()
s3 = S3Utility(config)
## The following is in DBP, and in valid_dbp
s3.downloadFile("dbp-prod", "text/GILBSP/GILBSP/GILBSP_8_JDG_1.html", "text:GILBSP:GILBSP:GILBSP_8_JDG_1.html")
## The following is in DBP, and not in valid_dbp
s3.downloadFile("dbp-prod", "text/GILBSP/GILBSP/GILBSP_26_LAM_1.html", "text:GILBSP:GILBSP:GILBSP_26_LAM_1.html")
"""





