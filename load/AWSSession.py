# AWSSession.py

import boto3
from Config import *
from botocore.config import Config as BotoConfig

class AWSSession:

	_instance = None
	def shared():
		if AWSSession._instance == None:
			AWSSession._instance = AWSSession()
		return AWSSession._instance


	def __init__(self):
		self.config = Config.shared()
		self.s3Client = self._securityTokenService("s3", "AssumeRoleSession1", "us-west-2")


	def elasticTranscoder(self):
		return self._securityTokenService("elastictranscoder", "AssumeRoleSession2", self.config.video_transcoder_region)


	def lambdaInvoker(self):
		return self._securityTokenService("lambda", "AssumeRoleSession3", self.config.lambda_zip_region, 
			self.config.lambda_zip_timeout)


	def _securityTokenService(self, clientType, roleSessionName, regionName, timeout=None):
		session = boto3.Session(profile_name = self.config.s3_aws_profile, region_name = regionName)
		if self.config.s3_aws_role == None:
			client = session.client(clientType)
		else:
			stsClient = session.client('sts')
			assumedRoleObject = stsClient.assume_role(
				RoleArn = self.config.s3_aws_role,
			    RoleSessionName = roleSessionName
			)
			credentials = assumedRoleObject['Credentials']
			if timeout != None:
				botoConfig = BotoConfig(read_timeout = timeout, connect_timeout = timeout)
				client = boto3.client(
			    	clientType,
			    	aws_access_key_id = credentials['AccessKeyId'],
			    	aws_secret_access_key = credentials['SecretAccessKey'],
			    	aws_session_token = credentials['SessionToken'],
			    	region_name = regionName,
			    	config = botoConfig
				)
			else:
				client = boto3.client(
				    clientType,
				    aws_access_key_id = credentials['AccessKeyId'],
				    aws_secret_access_key = credentials['SecretAccessKey'],
				    aws_session_token = credentials['SessionToken'],
				    region_name = regionName
				)				
			#print("Created role %s based session for %s." % (self.config.s3_aws_role, clientType))
		return client


	def profile(self):
		if self.config.s3_aws_role_profile != None:
			return "--profile %s" % (self.config.s3_aws_role_profile)
		elif self.config.s3_aws_profile != None:
			return "--profile %s" % (self.config.s3_aws_profile)
		else:
			return ""		


# Unit Test
if (__name__ == '__main__'):
	session = AWSSession.shared()
	#for bucket in session.s3Client.buckets.all():
	response = session.s3Client.list_buckets()
	for bucket in response['Buckets']:
		print(bucket['Name'])
	

