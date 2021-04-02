# RunStatus.py


import boto3
import time
from Config import *
from DBPRunFilesS3 import *


class RunStatus:
	BIBLE = "BIBLE"
	LPTS = "LPTS"

	start = time.time()
	statusMap = {}
	statusList = []


	def init(filesets):
		results = []
		results.append(RunStatus.BIBLE)
		for inp in filesets:
			results.append(inp.filesetId)
		results.append(RunStatus.LPTS)
		RunStatus.statusList = results
		RunStatus.store()		


	def set(name, status):
		msg = "ok" if status else "failed"
		RunStatus.statusMap[name] = msg
		print("********** %s Tables Update %s **********" % (name, msg,))
		RunStatus.store()


	def store():
		results = []
		for name in RunStatus.statusList:
			status = RunStatus.statusMap.get(name, "pending")
			results.append(name + ": " + status)
		statusMsg = ", ".join(results)
		client = Config.shared().s3_client
		bucket = Config.shared().s3_artifacts_bucket
		key = DBPRunFilesS3.s3KeyPrefix + "/metadata"
		metadata = {}
		try:
			response = client.get_object(Bucket=bucket, Key=key)
			metadata = response.get('Metadata')
		except client.exceptions.NoSuchKey:
			metadata = {}
		except Exception as err:
			print("RunStatus Metadata Download error", err)
		try:
			metadata["status"] = statusMsg
			#print("WRITE", metadata.get("status"))
			client.put_object(Bucket=bucket, Key=key, Metadata=metadata, ACL='bucket-owner-full-control')
		except Exception as err:
			print("RunStatus Metadata Upload error", err)


	def printDuration(status):
		now = time.time()
		duration = now - RunStatus.start
		print("********* " + status + " ********* ", round(duration, 1), "sec")
		RunStatus.start = now


if __name__ == "__main__":
	from LPTSExtractReader import *
	from InputFileset import *

	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	RunStatus.init(filesets)
	time.sleep(1)
	RunStatus.set(RunStatus.BIBLE, True)
	time.sleep(2)
	for fileset in filesets:
		RunStatus.set(fileset.filesetId, False)
		time.sleep(2)
	RunStatus.set(RunStatus.LPTS, False)

# python3 load/RunStatus.py test s3://test-dbp audio/ENGESV/ENGESVN2DA  audio/ENGESV/ENGESVN2DA16

