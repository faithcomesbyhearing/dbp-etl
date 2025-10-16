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
		session = boto3.Session(profile_name = self.config.s3_aws_profile, region_name = regionName)

		# for docker execution, assume_role_arn is provided by ECS task configuration
		if self.config.s3_aws_role_arn == None:
			print ("AWSSession...assume role arn is not provided. If this is not a docker execution, check dbp-etl.cfg and verify there is a value for s3.aws_role_arn (previously, it was s3.aws_role)")
			client_args = {
				'service_name': clientType,
			}

			# set endpoint_url if provided in config
			# this is used for local testing with docker container
			# and for testing with mock elastic transcoder
			if hasattr(self.config, 'video_transcoder_url') and self.config.video_transcoder_url is not None and clientType == "elastictranscoder":
				client_args['endpoint_url'] = self.config.video_transcoder_url
			if timeout != None:
				boto_config = BotoConfig(retries={'max_attempts': 0}, read_timeout = timeout, connect_timeout = timeout)
				client_args['config'] = boto_config

			client = session.client(**client_args)
			return client 
			
		# assume_role_arn explicitely provided
		stsClient = session.client('sts')
		print ("AWSSession. assume role arn: %s" % (self.config.s3_aws_role_arn) )
		assumedRoleObject = stsClient.assume_role(
			RoleArn = self.config.s3_aws_role_arn,
		    RoleSessionName = roleSessionName,
			DurationSeconds = 12*60*60
		)
		credentials = assumedRoleObject['Credentials']
		botoConfig = BotoConfig()
		if timeout != None:
			botoConfig = BotoConfig(retries={'max_attempts': 0}, read_timeout = timeout, connect_timeout = timeout)

		#print ("connect timeout: %s, read_timeout: %s" % (botoConfig.connect_timeout,botoConfig.read_timeout))
		client_args = {
			'service_name': clientType,
			'aws_access_key_id': credentials['AccessKeyId'],
			'aws_secret_access_key': credentials['SecretAccessKey'],
			'aws_session_token': credentials['SessionToken'],
			'region_name': regionName,
			'config': botoConfig
		}
		# set endpoint_url if provided in config
		# this is used for local testing with docker container
		# and for testing with mock elastic transcoder
		if hasattr(self.config, 'video_transcoder_url') and self.config.video_transcoder_url is not None and clientType == "elastictranscoder":
			client_args['endpoint_url'] = self.config.video_transcoder_url

		client = boto3.client(**client_args)
		print("Created role %s based session for %s." % (self.config.s3_aws_role_arn, clientType))
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
	

