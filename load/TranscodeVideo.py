# TranscodeVideo.py

import time
import csv
import boto3
from S3Utility import *


class TranscodeVideo:

	def transcodeVideoFilesets(config):
		directory = self.config.directory_database
		for typeCode in os.listdir(directory):
			if typeCode == "video":
				for bibleId in [f for f in os.listdir(directory + typeCode) if not f.startswith('.')]:
					for filesetId in [f for f in os.listdir(directory + typeCode + os.sep + bibleId) if not f.startswith('.')]:
						filesetPrefix = typeCode + "/" + bibleId + "/" + filesetId + "/"
						done = self.transcodeFileset(filesetPrefix)
						if done:
							print("Transcode %s succeeded." % (filesetPrefix))
						else:
							print("Transcode %s FAILED." % (filesetPrefix))


	def transcodeFileset(config, directory, filesetPrefix):
		transcoder = TranscodeVideo(config, filesetPrefix)
		for filename in [f for f in os.listdir(directory + filesetPrefix) if not f.startswith('.')]:
			transcoder.createJob(filename)
		return transcoder.completeJobs()


	## Consumed by UpdateDBPVideoTable
	def getHLSTypes():
		return ["_stream", "_av720p", "_av480p", "_av360p"]


	def __init__(self, config, filesetPrefix):
		self.config = config
		self.filesetPrefix = filesetPrefix
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('elastictranscoder', region_name=config.video_transcoder_region)
		self.videoPipeline = config.video_transcoder_pipeline
		self.hls_audio_video_1080p = config.video_preset_hls_1080p
		self.hls_audio_video_720p = config.video_preset_hls_720p
		self.hls_audio_video_480p = config.video_preset_hls_480p
		self.hls_audio_video_360p = config.video_preset_hls_360p
		self.web_audio_video = config.video_preset_web
		## production values
		#self.video_pipeline = '1537458645466-6z62tx'
		#self.hls_audio_video_1080p = '1556116949562-tml3vh'
		#self.hls_audio_video_720p = '1538163744878-tcmmai'
		#self.hls_audio_video_480p = '1538165037865-dri6c1'
		#self.hls_audio_video_360p = '1556118465775-ps3fba'
		#self.web_audio_video = '1351620000001-100070'
		self.openJobs = []


	def createJob(self, file):
		baseobj = file[:file.rfind(".")]
		inputs = [
			{
				"Key": file
			}
		]
		outputs = [
			{
				"Key": baseobj + "_av720p",
				"PresetId": self.hls_audio_video_720p,
				"SegmentDuration": "3"
			},
			{
				"Key": baseobj + "_av480p",
				"PresetId": self.hls_audio_video_480p,
				"SegmentDuration": "3"
			},
			{
				"Key": baseobj + "_av360p",
				"PresetId": self.hls_audio_video_360p,
				"SegmentDuration": "3"
			},
			{
				"Key": baseobj + "_web.mp4",
				"PresetId": self.web_audio_video
			}
		]
		playlists = [
			{
				"Name": baseobj + "_stream",
				"Format": "HLSv3",
				"OutputKeys": [
					baseobj + "_av720p",
					baseobj + "_av480p",
					baseobj + "_av360p"
				]
			}
		]
		status = self.client.create_job(PipelineId=self.videoPipeline,
										  Inputs=inputs,
										  Outputs=outputs,
										  Playlists=playlists)
		self.openJobs.append(status["Job"]["Id"])


	def completeJobs(self):
		errorCount = 0
		while len(self.openJobs) > 0:
			stillOpenJobs = []
			for jobId in self.openJobs:
				response = self.client.read_job(Id=jobId).get("Job")
				inputObj = response["Input"]["Key"]
				status = response.get("Status")
				print("check Job", status, inputObj)
				if status == "Complete":
					print("Job Complete", status, inputObj)

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
			time.sleep(15)
		return errorCount == 0	


if (__name__ == '__main__'):
	config = Config()
	testFileset = "video/ENGESV/ENGESVP2DV"
	transcoder = TranscodeVideo(config, testFileset)
	csvFilename = config.directory_accepted + "/" + testFileset.replace("/", "_") + ".csv"
	with open(csvFilename, newline='\n') as csvfile:
		reader = csv.DictReader(csvfile)
		count = 0
		for row in reader:
			if count < 1:
				transcoder.createJob(testFileset + "/" + row["file_name"])
				count += 1
	transcoder.completeJobs()


