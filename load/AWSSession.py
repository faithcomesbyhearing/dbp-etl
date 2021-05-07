# AWSSession.py

import boto3
from Config import *

class AWSSession:

	_instance = None
	def shared():
		if AWSSession._instance == None:
			AWSSession._instance = AWSSession()
		return AWSSession._instance


	def __init__(self):
		config = Config.shared()
		session = boto3.Session(profile_name=config.s3_aws_profile)
		if config.s3_aws_role == None:
			self.s3Client = session.client('s3')
		else:
			# The calls to AWS STS AssumeRole must be signed with the access key ID
			# and secret access key of an existing IAM user or by using existing temporary
			# credentials such as those from another role. (You cannot call AssumeRole
			# with the access key for the root account.) The credentials can be in
			# environment variables or in a configuration file and will be discovered
			# automatically by the boto3.client() function. For more information, see the
			# Python SDK documentation:
			# http://boto3.readthedocs.io/en/latest/reference/services/sts.html#client

			# create an STS client object that represents a live connection to the
			# STS service
			stsClient = session.client('sts')
			#print("client", stsClient)

			# Call the assume_role method of the STSConnection object and pass the role
			# ARN and a role session name.
			assumedRoleObject = stsClient.assume_role(
				RoleArn = config.s3_aws_role,
			    RoleSessionName = "AssumeRoleSession1"
			)
			#print("role", assumedRoleObject)

			# From the response that contains the assumed role, get the temporary
			# credentials that can be used to make subsequent API calls
			credentials = assumedRoleObject['Credentials']
			#print("credentials", credentials)

			# Use the temporary credentials that AssumeRole returns to make a
			# connection to Amazon S3
			#self.s3Client = boto3.resource(
			self.s3Client = boto3.client(
			    's3',
			    aws_access_key_id = credentials['AccessKeyId'],
			    aws_secret_access_key = credentials['SecretAccessKey'],
			    aws_session_token = credentials['SessionToken'],
			)



# Unit Test
if (__name__ == '__main__'):
	session = AWSSession.shared()
	#for bucket in session.s3Client.buckets.all():
	response = session.s3Client.list_buckets()
	for bucket in response['Buckets']:
    	#buckets += {bucket["Name"]}
		print(bucket['Name'])
	

