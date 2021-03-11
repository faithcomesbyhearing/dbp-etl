
import os
import sys
import time
import boto3
import boto3.s3.transfer
from Config import *


class S3Test:

	def __init__(self):
		self.config = Config()
		self.bucket = self.config.s3_bucket
		session = boto3.Session(profile_name=self.config.s3_aws_profile)
		self.client = session.client('s3')
		self.results = []


	def syncTest(self, filesetPrefix):
		start = time.time()
		cmd = "aws s3 sync %s%s s3://%s/%s" % (self.config.directory_upload, filesetPrefix, self.bucket, filesetPrefix)
		os.system(cmd)
		self.duration(start, "aws s3 sync:")


	def cpTest(self, filesetPrefix):
		start = time.time()
		cmd = "aws s3 cp %s%s s3://%s/%s --recursive" % (self.config.directory_upload, filesetPrefix, self.bucket, filesetPrefix)
		os.system(cmd)
		self.duration(start, "aws s3 cp:")


	def boto3Test(self, filesetPrefix):
		start = time.time()
		directory = self.config.directory_upload + filesetPrefix
		for file in os.listdir(directory):
			filename = directory + file
			s3Key = filesetPrefix + file
			print("upload", s3Key, filename)
			if file.endswith(".mp3"):
				self.client.upload_file(filename, self.bucket, s3Key,
				ExtraArgs={'ContentType': "audio/mpeg", 'ACL': 'bucket-owner-full-control'})
			elif ! os.path.isdir(filename):
				self.client.upload_file(filename, self.bucket, s3Key, ExtraArgs={'ACL': 'bucket-owner-full-control'})
		self.duration(start, "boto3 s3.upload_file:")


	def transferTest(self, filesetPrefix):
		start = time.time()
		transConfig = boto3.s3.transfer.TransferConfig()
		transfer = boto3.s3.transfer.S3Transfer(client=self.client, config=transConfig, osutil=None, manager=None)
		directory = self.config.directory_upload + filesetPrefix
		for file in os.listdir(directory):
			filename = directory + file
			s3Key = filesetPrefix + file
			print("upload", s3Key, filename)		
			transfer.upload_file(filename, self.bucket, s3Key, callback=None, extra_args=None)
		self.duration(start, "boto3 s3.transfer:")


	def getFilesetList(self):
		filesetList = []
		directory = self.config.directory_upload
		for typeCode in os.listdir(directory):
			if typeCode == "audio":
				for bibleId in os.listdir(directory + "/audio"):
					for filesetId in os.listdir(directory + "/audio/" + bibleId):
						filesetList.append(typeCode + "/" + bibleId + "/" + filesetId + "/")
		return(filesetList)


	def duration(self, start, testName):
		duration = time.time() - start
		self.results.append("TIME %s %s sec" % (testName, "{:.1f}".format(duration)))
		#print(testName, "{:.3f}".format(duration), "sec")


if (__name__ == '__main__'):
	test = S3Test()
	filesetList = test.getFilesetList()
	filesetPrefix = filesetList[0]
	test.syncTest(filesetPrefix)
	test.cpTest(filesetPrefix)
	test.boto3Test(filesetPrefix)
	test.transferTest(filesetPrefix)
	for result in test.results:
		print(result)

