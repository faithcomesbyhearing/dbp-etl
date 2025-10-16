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
		return self._securityTokenService("lambda", "AssumeRoleSession3", self.config.lambda_zip_region, self.config.lambda_zip_timeout)


	def ecsClient(self):
		return self._securityTokenService("ecs", "AssumeRoleSession4", "us-west-2")



	def _securityTokenService(self, clientType, roleSessionName, regionName, timeout=None):
		print("================> AWSSession...creating %s client in region %s " % (clientType, regionName))
		session = boto3.Session(region_name=regionName) # here we have an STS session based on the role provided by ECS

		sts_client = session.client("sts")
		response = sts_client.get_caller_identity()
		print("================> LINE 38 using role from ECS task")
		print(response)

		client = session.client(clientType)

		client_args = {
			'service_name': clientType,
		}

		client = session.client(**client_args)

		# Print the response (similar to CLI output)
		return client


	def role_profile(self):
		if self.config.s3_aws_role_profile != None:
			return "--profile %s" % (self.config.s3_aws_role_profile)
		else:
			return ""		
	

# Unit Test
if (__name__ == '__main__'):
	session = AWSSession.shared()
	request = { 'Bucket': 'etl-development-input', 'MaxKeys': 10 }
	response = session.s3Client.list_objects_v2(**request)
	for item in response.get('Contents', []):
		objKey = item.get('Key')
		print (objKey)
	

