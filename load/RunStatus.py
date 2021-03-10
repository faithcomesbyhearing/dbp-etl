# RunStatus.py



import boto3
import time
from Config import *
from DBPRunFilesS3 import *


class RunStatus:
	UNKNOWN = "UNKNOWN"
	PREPROCESS = "BEGIN PREPROCESS"
	VALIDATE = "BEGIN VALIDATE"
	UPLOAD = "BEGIN UPLOAD"
	UPDATE = "BEGIN UPDATE"
	SUCCESS = "SUCCESS"
	FAILURE = "FAILURE"

	current = UNKNOWN
	start = time.time()


	def setStatus(status):
		RunStatus.current = status
		RunStatus.printDuration(status)
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
			metadata["status"] = RunStatus.current
			client.put_object(Bucket=bucket, Key=key, Metadata=metadata)
		except Exception as err:
			print("RunStatus Metadata Upload error", err)


	def printDuration(status):
		now = time.time()
		duration = now - RunStatus.start
		print("********* " + status + " ********* ", round(duration, 1), "sec")
		RunStatus.start = now



if __name__ == "__main__":
	time.sleep(1)
	RunStatus.setStatus(RunStatus.VALIDATE)
	time.sleep(2)
	RunStatus.setStatus(RunStatus.UPLOAD)
	time.sleep(3)
	RunStatus.printDuration("TRANSCODE SUBMIT")
	time.sleep(2)
	RunStatus.printDuration("TRANSCODE CHECK")
	time.sleep(1)
	RunStatus.printDuration("TRANSCODE DONE")

