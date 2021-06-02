# ZipUtility

import os
import sys
import zipfile
import boto3
import subprocess
from Config import *
from AWSSession import *


class ZipUtility:


	def __init__(self, config):
		self.config = config  #### This is not needed is it.


	def zipAudio(self, bucket, filesetPrefix):
		isOK = False
		directory = self.downloadFileset(bucket, filesetPrefix)
		if directory != None:
			zipFile = self.zipAudioFileset(directory)
			zipObjName = filesetPrefix + os.sep + os.path.basename(filesetPrefix) + ".zip"
			isOK = self.uploadZipFile(bucket, zipObjName, zipFile)
		return isOK


	def downloadFileset(self, bucket, filesetPrefix):
		profile = AWSSession.shared().profile()
		if not bucket.startswith("s3://"):
			bucket = "s3://" + bucket
		source = bucket + "/" + filesetPrefix
		target = "/tmp/" + os.path.basename(filesetPrefix)
		cmd = "aws %s s3 sync --acl bucket-owner-full-control %s %s" % (profile, source, target)
		print("upload:", cmd)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0:
			print("ERROR: Download of %s to %s failed. MESSAGE: %s" % (source, target, response.stderr))
			return None
		else:
			return target


	def zipAudioFileset(self, directory):
		zipfilePath = "/tmp/result.zip"
		if os.path.isfile(zipfilePath):
			os.remove(zipfilePath)
		zipDir = zipfile.ZipFile(zipfilePath, "w")
		with zipDir:
			for file in os.listdir(directory):
				if file.endswith(".mp3") and not file.startswith("."):
					fullPath = directory + os.sep + file
					zipDir.write(fullPath, file)
		return zipfilePath


	def uploadZipFile(self, bucket, zipObjName, zipFile):
		try:
			response = AWSSession.shared().s3Client.upload_file(zipFile, bucket, zipObjName)
		except ClientError as e:
			print("ERROR: Upload of %s to %s/%s failed." % (zipFile, bucket, zipObjName))
			return False
		return True


	def downloadZipFile(self, bucket, zipObjName, zipFile):
		try:
			response = AWSSession.shared().s3Client.download_file(bucket, zipObjName, zipFile)
		except ClientError as e:
			print("ERROR: Download of %s/%s to %s failed." % (bucket, zipObjName, zipFile))


if (__name__ == '__main__'):
	config = Config.shared()
	zip = ZipUtility(config)
	filesetPrefix = "audio/UNRWFW/UNRWFWP1DA"
	zip.zipAudio("dbp-staging", filesetPrefix)
	zipObjName = filesetPrefix + os.sep + os.path.basename(filesetPrefix) + ".zip"
	zip.downloadZipFile("dbp-staging", zipObjName, "./test.zip")





