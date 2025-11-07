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
		if self.config.s3_aws_profile != None:
			print("AWSSession...INIT creating Session with profile: %s" % (self.config.s3_aws_profile))
			self.session = boto3.Session(region_name="us-west-2", profile_name=self.config.s3_aws_profile)
		else:
			print("AWSSession...INIT creating Session with default ecs-task-role")
			self.session = boto3.Session(region_name="us-west-2")

		# Get identity information using STS
		sts_client = self.session.client("sts")
		identity = sts_client.get_caller_identity()

		arn = identity.get('Arn', '')
		session_name = arn.split('/')[-1] if '/' in arn else 'N/A'

		print("================> Session Identity:")
		print("    Account: %s" % identity.get('Account'))
		print("    UserId: %s" % identity.get('UserId'))
		print("    Arn: %s" % arn)
		print("    Session Name: %s" % session_name)

		print("================> AWSSession...END creating Session with ecs-task-role")
		self.s3Client = self._securityTokenService("s3")


	def elasticTranscoder(self):
		return self._securityTokenService("elastictranscoder")

	def lambdaInvoker(self):
		return self._securityTokenService("lambda")

	def ecsClient(self):
		return self._securityTokenService("ecs")

	def _securityTokenService(self, clientType):
		print("_securityTokenService AWSSession...creating %s client" % (clientType))

		client = self.session.client(clientType)
		return client

# Unit Test
if (__name__ == '__main__'):
	session = AWSSession.shared()
	request = { 'Bucket': 'etl-development-input', 'MaxKeys': 10 }
	response = session.s3Client.list_objects_v2(**request)
	for item in response.get('Contents', []):
		objKey = item.get('Key')
		print (objKey)
	

