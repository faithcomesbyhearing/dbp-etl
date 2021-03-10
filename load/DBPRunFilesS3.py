# UploadFileS3.py

import os
from datetime import datetime
import boto3
from Config import *

class DBPRunFilesS3:

	s3KeyPrefix = os.getenv('S3_KEY_PREFIX') or datetime.today().strftime("%Y-%m-%d-%H-%M-%S")


	def uploadFile(config, filepath):
		s3Key = os.path.basename(filepath)
		DBPRunFilesS3.uploadObject(config, s3Key, filepath, "text/plain")


	def uploadParsedCSV(config, filepath):
		key = os.path.basename(filepath)
		if config.directory_accepted in filepath:
			s3Key = key[:-4] + "-accepted.csv"
		else:
			s3Key = key[:-4] + "-rejected.csv"
		DBPRunFilesS3.uploadObject(config, s3Key, filepath, "text/cvs")


	def uploadObject(config, key, filepath, contentType):
		s3Key = DBPRunFilesS3.s3KeyPrefix + "/" + key
		session = boto3.Session(profile_name=config.s3_aws_profile)
		client = session.client('s3')
		bucket = config.s3_artifacts_bucket
		#print(s3Key, filepath)
		try:
			client.upload_file(filepath, bucket, s3Key, ExtraArgs={'ContentType': contentType})
		except Exception as err:
			print("ERROR: Upload %s failed  with error %s" % (s3Key, err))


	def __init__(self, config):
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')
		self.bucket = config.s3_artifacts_bucket


	def listRuns(self, year=None, month=None, day=None):
		runDates = []
		if year != None and month != None and day != None:
			startWith = "%04d-%02d-%02d-" % (year, month, day) 
		elif year != None and month != None:
			startWith = "%04d-%02d-" % (year, month)
		elif year != None:
			startWith = "%04d-" % (year)
		else:
			startWith = "0"
		print("start", startWith)
		request = { 'Bucket': self.bucket, 'Delimiter': '/', 'StartAfter': startWith, 'MaxKeys': 1000 }
		hasMore = True
		while hasMore:
			response = self.client.list_objects_v2(**request)
			for prefix in response.get('CommonPrefixes', []):
				runDates.append(prefix.get('Prefix'))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		return runDates


	def listRunFiles(self, prefix):
		objects = []
		request = { 'Bucket': self.bucket, 'MaxKeys':1000, 'Prefix': prefix }
		hasMore = True
		while hasMore:
			response = self.client.list_objects_v2(**request)
			for item in response.get('Contents', []):
				objects.append(item.get('Key'))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		return objects


	def downloadObjects(self, directory, prefix, objects):
		path = directory + os.sep + prefix
		if not os.path.exists(path):
			os.mkdir(path)
		for key in objects:
			try:
				filename = directory + "/" + key
				print("Download %s to %s" % (key, filename))
				self.client.download_file(self.bucket, key, filename)
				#print("Done With Download")
			except Exception as err:
				print("ERROR: Download %s failed with error %s" % (key, err))



if (__name__ == '__main__'):

	config = Config()
	run = DBPRunFilesS3(config)

	runtype = sys.argv[2] if len(sys.argv) > 2 else None
	if runtype == "list":
		print("Usage DBPRunFilesS3  config_profile  list  [year]  [month]  [day]")
		year = int(sys.argv[3]) if len(sys.argv) > 3 else None
		month = int(sys.argv[4]) if len(sys.argv) > 4 else None
		day = int(sys.argv[5]) if len(sys.argv) > 5 else None
		runs = run.listRuns(year, month, day)
		for run in runs:
			print(run)

	elif runtype == "test":
		DBPRunFilesS3.uploadFile(config, "Trans-21-02-09-23-24-14-ENGESVN2DA.sql")
		DBPRunFilesS3.uploadFile(config, "/Volumes/FCBH/files/errors//Errors-21-02-12-17-20-44.out")
		DBPRunFilesS3.uploadParsedCSV(config, "/Volumes/FCBH/files/active/accepted/21-02-11-18-01-18/text_GNWNTM_GNWNTM.csv")

	else:
		print("Usage DBPRunFilesS3  config_profile  run-date-time | list")
		if len(sys.argv) < 3:
			print("run-date-time is required")
			sys.exit() 
		directory = "."
		prefix = sys.argv[2]
		print(sys.argv)
		objects = run.listRunFiles(prefix)
		#print(objects)
		run.downloadObjects(directory, prefix, objects)




