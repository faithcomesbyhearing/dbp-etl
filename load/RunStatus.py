# RunStatus.py



import boto3
from Config import *
from DBPRunFilesS3 import *


class RunStatus:
	UNKNOWN = "UNKNOWN"
	PREPROCESS = "PREPROCESS"
	VALIDATE = "VALIDATE"
	UPLOAD = "UPLOAD"
	UPDATE = "UPDATE"
	SUCCESS = "SUCCESS"
	FAILURE = "FAILURE"

	current = UNKNOWN


	def setStatus(status):
		RunStatus.current = status
		client = Config.shared().s3_client
		bucket = Config.shared().s3_artifacts_bucket
		key = DBPRunFilesS3.s3KeyPrefix + "/metadata"
		metadata = {}
		try:
			response = client.get_object(Bucket=bucket, Key=key)
			metadata = response.get('Metadata')
			#print(metadata.get("x-run-status"))
		except client.exceptions.NoSuchKey:
			metadata = {}
		except Exception as err:
			print("RunStatus Metadata Download error", err)
		try:
			metadata["x-run-status"] = RunStatus.current
			client.put_object(Bucket=bucket, Key=key, Metadata=metadata)
		except Exception as err:
			print("RunStatus Metadata Upload error", err)



if __name__ == "__main__":
	RunStatus.setStatus(RunStatus.VALIDATE)

