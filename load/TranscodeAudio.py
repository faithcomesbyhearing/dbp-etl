# Transcoder.py

import time
import boto3
from S3Utility import *


class TranscodeAudio:


	def __init__(self, config, filesetPrefix):
		self.config = config
		self.filesetPrefix = filesetPrefix
		region = "us-east-1"  # put in config
		self.client = boto3.client('elastictranscoder', region_name=region)
		self.audioTranscoder = '1599766493865-viabwr'
		self.preset_mp3_16bit = '1600051328798-010nb3'
		self.preset_mp3_32bit = '1600051390966-jwg0ig'
		self.preset_mp3_64bit = '1600051427984-acsojp'
		self.preset_mp3_128bit = '1351620000001-300040'
		self.submittedJobs = []


	def createMP3Job(self, file):
		baseobj = file[:file.rfind(".")]
		underscore = baseobj.find("_")
		if underscore > 9:
			output = baseobj[:underscore] + "%s" + baseobj[underscore:] + ".mp3"
		else:
			output = baseobj + "%s.mp3"
		inputs = [{
			"Key": file,
			"Container": "mp3"
		}]
		outputs = [
		{
			"Key": output % ("16"),
			"PresetId": self.preset_mp3_16bit,
	
		},
		{
			"Key": output % ("32"),
			"PresetId": self.preset_mp3_32bit,
		
		},
		{
			"Key": output % ("64"),
			"PresetId": self.preset_mp3_64bit,
		
		},
		{
			"Key": output % ("128"),
			"PresetId": self.preset_mp3_128bit
		}]
		status = self.client.create_job(PipelineId=self.audioTranscoder,
										  Inputs=inputs,
										  Outputs=outputs)
		#return status["Job"]["Id"]
		self.submittedJobs.append(status["Job"]["Id"])


	## As the transcode succeed for each, the source files can be promoted to uploaded.
	## We need the filesize and duration information for the the update process
	## The filesize and duration need to be put into flat files so that it can process
	## What do I do with error information

	## It should generate the file when all is completed
	def completeJobs(self):
		moreToDo = True
		while moreToDo:
		for jobId in self.submittedJobs:
			statusList = self.getJobStatus(jobId)
			try:
				status = transcoder_client.read_job(Id=job)
				if status['Job']['Status'] == 'Complete' or status['Job']['Status'] == 'Error':
					fName=None
					fDuration=None
					try:
						fName=status['Job']['Input']['Key']
					except:
						print("Could not set file name")
					try:
						fDuration=status['Job']['Output']['Duration']
					except:
						print("Could not set file duration")
					
					if fName is not None and fDuration is not None:
						videoFileDic.update({fName:fDuration})
					jList.remove(job)
			except:
				print("Could not get status for: "+job)
		if len(jList)==0:
			isDone=True	

		## This must return true or false whether the task is completed correctly	



	def getJobStatus(self, jobId):
		response = self.client.read_job(Id=jobId).get("Job")
		status = response.get("Status")
		# status := Submitted | Progressing | Complete | Canceled | Error
		print(status)
		if status == "Complete":
			outputs = response.get("Outputs", [])
			for output in outputs:
				objNameOut = output.get("Key")
				statusOut = output.get("Status")
				fileSize = output.get("FileSize")
				duration = output.get("Duration")
				durationMS = output.get("DurationMillis")
				print(statusOut, objNameOut, fileSize, duration, durationMS)
				return()

		elif status == "Error":
			outputs = response.get("Outputs", [])
			for output in outputs:
				objNameOut = output.get("Key")
				statusOut = output.get("Status")
				statusDetail = output.get("StatusDetail")
				print(objNameOut, statusOut, statusDetail)

		else:
			return None


	def createAcceptedFiles(self):
		print("TBD")

		


if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	testFileset = "audio/ENGESV/ENGESVN2DA"
	transcoder = TranscodeAudio(config, testFileset)
	# open accepted file
	# read lines
	# call create job for each line
		transcoder.createMP3Job("B01___01_Matthew_____ENGESVN1DA.mp3")
	transcoder.completeJobs()
	#transcoder.getJobStatus(jobId)
	#transcoder.createMP3Job("ENGESVN2DA_B03_LUK_003_04-003_08.mp3")

		## createMP3Job will be called by S3Utility in production
	def unitTestCreateMP3Jobs(self):
		print("TBD")
		# open accepted file for this fileset
		# process lines one at a time, and call createMP3


