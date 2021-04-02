# TranscodeVideo.py

import time
import csv
import boto3
from S3Utility import *
from RunStatus import *


class TranscodeVideo:


	def transcodeVideoFileset(config, filesetPrefix, s3FileKeys):
		transcoder = TranscodeVideo(config, filesetPrefix)
		RunStatus.printDuration("BEGIN SUBMIT TRANSCODE")
		for s3FileKey in s3FileKeys:
			transcoder.createJob(s3FileKey)
		RunStatus.printDuration("BEGIN CHECK TRANSCODE")
		done = transcoder.completeJobs()
		if done:
			print("Transcode %s succeeded." % (filesetPrefix))
		else:
			print("Transcode %s FAILED." % (filesetPrefix))
		RunStatus.printDuration("TRANSCODE DONE")


	## Used by UpdateDBPVideoTable
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
				if status == "Complete":
					print("Job Complete", inputObj)

				elif status == "Error":
					output = response.get("Output", {})
					errorMsg = output.get("StatusDetail", "")
					if not errorMsg.startswith("3002"):
						errorCount += 1
					for output in response.get("Outputs", []):
						if output.get("Status") == "Error":
							print("ERROR %s" % (output.get("StatusDetail")))
						else:
							print("NO Message", output.get("Key"), output.get("StatusDetail"))

				else:
					stillOpenJobs.append(jobId)
			self.openJobs = stillOpenJobs
			time.sleep(10)
		return errorCount == 0	


if (__name__ == '__main__'):
	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	for inp in filesets:
		if inp.typeCode == "video":
			TranscodeVideo.transcodeVideoFileset(config, inp.filesetPrefix, inp.s3FileKeys())

# Successful tests with source on local drive
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3



