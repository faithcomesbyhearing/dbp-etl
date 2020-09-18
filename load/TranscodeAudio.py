# Transcoder.py

import time
import csv
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
		self.openJobs = []


	def createMP3Job(self, file):
		print("transcode", self.filesetPrefix + "/" + file)
		inputs = [{
			"Key": self.filesetPrefix + "/" + file,
			"Container": "mp3"
		}]
		outputs = [
		{
			"Key": self.filesetPrefix + "16/" + file,
			"PresetId": self.preset_mp3_16bit
		},
		{
			"Key": self.filesetPrefix + "32/" + file,
			"PresetId": self.preset_mp3_32bit
		},
		{
			"Key": self.filesetPrefix + "64/" + file,
			"PresetId": self.preset_mp3_64bit
		}]
		status = self.client.create_job(PipelineId=self.audioTranscoder,
										  Inputs=inputs,
										  Outputs=outputs)
		self.openJobs.append(status["Job"]["Id"])


	def completeJobs(self):
		results16 = []
		results32 = []
		results64 = []
		errorCount = 0
		while len(self.openJobs) > 0:
			stillOpenJobs = []
			for jobId in self.openJobs:
				response = self.client.read_job(Id=jobId).get("Job")
				inputObj = response["Input"]["Key"]
				status = response.get("Status")
				print("check Job", jobId, status, inputObj)
				if status == "Complete":
					outputs = response["Outputs"]
					self.appendOutputs(results16, outputs[0])
					self.appendOutputs(results32, outputs[1])
					self.appendOutputs(results64, outputs[2])

				elif status == "Error":
					errorCount += 1
					for output in response.get("Outputs", []):
						if output.get("Status") == "Error":
							print("ERROR %s" % (output.get("StatusDetail")))
						else:
							print("NO Message", output.get("Key"), output.get("StatusDetail"))

				else:
					stillOpenJobs.append(jobId)
			self.openJobs = stillOpenJobs
			time.sleep(5)
		if errorCount == 0:
			self.createTranscoded("MP3", "16", results16)
			self.createTranscoded("MP3", "32", results32)
			self.createTranscoded("MP3", "64", results64)


	def appendOutputs(self, results, output):
		filename = output.get("Key").split("/")[-1]
		results.append((filename, output.get("FileSize"), output.get("Duration")))


	def createTranscoded(self, codecType, bitrate, results):
		#cvsFilename = self.directory_accepted + self.filesetPrefix.replace("/", "_") + bitrate + ".csv"
		cvsFilename = self.filesetPrefix.replace("/", "_") + bitrate + ".csv"
		with open(cvsFilename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(("file_name", "file_size", "duration"))
			for row in results:
				writer.writerow(row)



if (__name__ == '__main__'):
	config = Config()
	testFileset = "audio/ENGESV/ENGESVN2DA"
	transcoder = TranscodeAudio(config, testFileset)
	csvFilename = config.directory_accepted + "/" + testFileset.replace("/", "_") + ".csv"
	with open(csvFilename, newline='\n') as csvfile:
		reader = csv.DictReader(csvfile)
		count = 0
		for row in reader:
			if count < 3000:
				transcoder.createMP3Job(row["file_name"])
				count += 1
	transcoder.completeJobs()


