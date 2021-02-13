# UploadFileS3.py

import os
from datetime import datetime
import boto3
from Config import *

class UploadRunFileS3:

	s3KeyPrefix = datetime.today().strftime("%Y-%m-%d-%H-%M-%S")

	def uploadFile(config, filepath):
		s3Key = os.path.basename(filepath)
		UploadRunFileS3.uploadObject(config, s3Key, filepath, "text/plain")

	def uploadParsedCSV(config, filepath):
		key = os.path.basename(filepath)
		if config.directory_accepted in filepath:
			s3Key = key[:-4] + "-accepted.csv"
		else:
			s3Key = key[:-4] + "-rejected.csv"
		UploadRunFileS3.uploadObject(config, s3Key, filepath, "text/cvs")

	def uploadObject(config, key, filepath, contentType):
		s3Key = UploadRunFileS3.s3KeyPrefix + "/" + key
		session = boto3.Session(profile_name=config.s3_aws_profile)
		client = session.client('s3')
		bucket = config.s3_artifacts_bucket
		print(s3Key, filepath)
		try:
			client.upload_file(filepath, bucket, s3Key, ExtraArgs={'ContentType': contentType})
		except Exception as err:
			print("ERROR: Upload %s failed  with error %s" % (s3Key, err))


if (__name__ == '__main__'):
	config = Config()
	UploadRunFileS3.uploadFile(config, "Trans-21-02-09-23-24-14-ENGESVN2DA.sql")
	UploadRunFileS3.uploadFile(config, "/Volumes/FCBH/files/errors//Errors-21-02-12-17-20-44.out")
	UploadRunFileS3.uploadParsedCSV(config, "/Volumes/FCBH/files/active/accepted/21-02-11-18-01-18/text_GNWNTM_GNWNTM.csv")
