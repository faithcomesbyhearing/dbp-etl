# RunStatus.py


import sys
import boto3
import time
import pwd
from Config import *
from DBPRunFilesS3 import *
from AWSSession import *
from SQLUtility import *
from Log import *


class RunStatus:
	BIBLE = "BIBLE"
	LPTS = "LPTS"
	NOT_DONE = "not done"

	start = time.time()
	statusMap = { BIBLE: NOT_DONE, LPTS: NOT_DONE }
	dbConn = None
	runId = None


	def add(inputFileset):
		RunStatus.statusMap[inputFileset.filesetId] = RunStatus.NOT_DONE
		if RunStatus.dbConn == None:
			config = Config.shared()
			RunStatus.dbConn = SQLUtility(config)
			sql = "INSERT INTO run_history (username, location, directory) VALUES (%s, %s, %s)"
			username = pwd.getpwuid(os.getuid()).pw_name
			RunStatus.runId = RunStatus.dbConn.executeInsert(sql, (username, inputFileset.locationForS3(), inputFileset.filesetPath))
		sql = "INSERT INTO run_batch (run_id, batch, status) VALUES (%s, %s, -1)"
		RunStatus.dbConn.execute(sql, (RunStatus.runId, inputFileset.filesetId))


	def set(name, status):
		msg = "ok" if status else "failed"
		RunStatus.statusMap[name] = msg
		print("********** %s Tables Update %s **********" % (name, msg,))
		RunStatus.store()
		if name != RunStatus.BIBLE and name != RunStatus.LPTS:
			sql = "UPDATE run_batch SET status = %s WHERE run_id = %s AND batch = %s"
			statusNum = 1 if status else -1
			RunStatus.dbConn.execute(sql, (statusNum, RunStatus.runId, name))


	def exit():
		errors = []
		for key in sorted(Log.loggers.keys()):
			logger = Log.loggers[key]
			for message in logger.format():
				errors.append(message)
		if RunStatus.dbConn != None:
			if len(errors) > 0:
				message = "\n".join(errors)
				RunStatus.dbConn.execute("UPDATE run_history SET errors = %s WHERE run_id = %s", (message, RunStatus.runId))
			RunStatus.dbConn.close()
		if all(msg == "ok" for msg in RunStatus.statusMap.values()):
			print("All Success in RunStatus")
			sys.exit()
		else:
			print("Error in RunStatus %s" % (RunStatus.statusMap))
			sys.exit(1)


	def store():
		result = []
		for (name, value) in RunStatus.statusMap.items():
			result.append("%s=%s" % (name, value))
		statusMsg = ", ".join(result)
		client = AWSSession.shared().s3Client
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
	s3Client = AWSSession.shared().s3Client
	filesets = InputFileset.filesetCommandLineParser(config, s3Client, lptsReader)
	time.sleep(1)
	RunStatus.set(RunStatus.BIBLE, False)
	time.sleep(2)
	for fileset in filesets:
		Log.getLogger(fileset.filesetId).message(Log.EROR, "This is a test error")
		RunStatus.set(fileset.filesetId, False)
		time.sleep(2)
	RunStatus.set(RunStatus.LPTS, False)
	RunStatus.exit()

# python3 load/RunStatus.py test s3://dbp-etl-artifacts-dev audio/ENGESV/ENGESVN2DA ENGESVO2DA

