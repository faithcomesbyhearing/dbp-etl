# S3Utility
#
# This file performs the S3Upload operations
#
#

import boto3
import os
import shutil
import subprocess
from Config import *
from InputFileset import *
from TranscodeVideo import *

class S3Utility:

	def __init__(self, config):
		self.config = config
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')


	def getAsciiObject(self, s3Bucket, s3Key):
		obj = self.client.get_object(Bucket=s3Bucket, Key=s3Key)
		content = obj['Body'].read().decode('ascii')
		return content


	def uploadAllFilesets(self, filesets):
		for inp in filesets:
			s3Bucket = self.config.s3_vid_bucket if inp.typeCode == "video" else self.config.s3_bucket
			if inp.typeCode in {"audio", "video"}:
				done = self.uploadFileset(s3Bucket, inp)
				if done:
					print("Upload %s succeeded." % (inp.filesetPrefix,))
				else:
					print("Upload %s FAILED." % (inp.filesetPrefix,))
							
			elif inp.typeCode == "text":
				InputFileset.database.append(inp)


	def uploadFileset(self, s3Bucket, inputFileset):
		inp = inputFileset
		if self.config.s3_aws_profile == None:
			profile = ""
		else:
			profile = "--profile %s" % (self.config.s3_aws_profile)
		if inp.locationType == InputFileset.LOCAL:
			source = inp.fullPath()
		else:
			source = "s3://%s" % (inp.fullPath())
		target = "s3://%s/%s" % (s3Bucket, inp.filesetPrefix)
		cmd = "aws %s s3 sync %s %s" % (profile, source, target)
		print("upload:", cmd)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0:
			print("ERROR: Upload of %s to %s failed. MESSAGE: %s" % (inp.filesetPrefix, s3Bucket, response.stderr))
			return False
		else:
			if inp.filesetPrefix.startswith("video"):
				TranscodeVideo.transcodeVideoFileset(self.config, inp.filesetPrefix, inp.s3FileKeys())
			InputFileset.database.append(inp)
			return True


if (__name__ == '__main__'):
	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	s3 = S3Utility(config)
	s3.uploadAllFilesets(filesets)

# Successful tests with source on local drive
# time python3 load/S3Utility.py test /Volumes/FCBH/all-dbp-etl-test/ ENGESVN2DA ENGESVN2DA16
# time python3 load/S3Utility.py test /Volumes/FCBH/all-dbp-etl-test/ HYWWAVN2ET
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3
# time python3 load/S3Utility.py test s3://test-dbp-etl ENGESVN2DA
# time python3 load/S3Utility.py test s3://test-dbp-etl text/ENGESV/ENGESVN2DA16
# time python3 load/S3Utility.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/S3Utility.py test s3://test-dbp-etl ENGESVP2DV

