# TranscodeVideo.py

import json
import boto3
from S3Utility import *
from RunStatus import *
from AWSSession import *

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
		self.videoPipeline = config.video_transcoder_pipeline
		self.hls_audio_video_1080p = config.video_preset_hls_1080p
		self.hls_audio_video_720p = config.video_preset_hls_720p
		self.hls_audio_video_480p = config.video_preset_hls_480p
		self.hls_audio_video_360p = config.video_preset_hls_360p
		self.web_audio_video = config.video_preset_web
		self.web_audio_video = config.video_preset_web
		self.client = self.getTranscoderClient()
		self.openJobs = []

	def getTranscoderClient(self):
		if hasattr(self.config, 'video_transcoder_mock') and self.config.video_transcoder_mock != None:
			try:
				self.video_transcoder_mock = json.loads(self.config.video_transcoder_mock)
			except json.JSONDecodeError:
				# Handle the error and set a empty dict for self.video_transcoder_mock
				self.video_transcoder_mock = {}

			if self.video_transcoder_mock.get("enable") == 1:
				return boto3.client(
					'elastictranscoder',
					region_name=self.video_transcoder_mock.get("region_name"),
					endpoint_url=self.video_transcoder_mock.get("endpoint_url")
				)

		return AWSSession.shared().elasticTranscoder()

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
	from LanguageReaderCreator import LanguageReaderCreator
	from InputProcessor import *

	config = Config.shared()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)

	for inp in filesets:
		if inp.typeCode == "video":
			TranscodeVideo.transcodeVideoFileset(config, inp.filesetPrefix, inp.s3FileKeys())

# Successful tests with source on local drive
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3


# python3 load/DBPLoadController.py test s3://etl-development-input/ "ENGESVP2DV"
