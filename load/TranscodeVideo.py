# TranscodeVideo.py

from itertools import count
import time
from S3Utility import *
from RunStatus import *
from AWSSession import *
from BibleBrainServiceTranscoder import BibleBrainServiceTranscoder
class TranscodeVideo:


	def transcodeVideoFileset(config, filesetPrefix, s3FileKeys, sourceBucket=None, filesetPath=None):
		# If ECS transcoder is enabled, use it, otherwise use Elastic Transcoder
		if config.transcoder_ecs_disabled is False:
			return BibleBrainServiceTranscoder.transcodeVideoFilesetECS(config, filesetPrefix, s3FileKeys, sourceBucket, filesetPath)
		else:
			return TranscodeVideo.transcodeVideoFilesetET(config, filesetPrefix, s3FileKeys)

	def transcodeVideoFilesetET(config, filesetPrefix, s3FileKeys):
		"""Transcode video fileset using Elastic Transcoder."""

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

		return done


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
		"""Returns the Elastic Transcoder client.
		If it is mocked, it will return the mock client.
		Otherwise, it will return the real AWS Elastic Transcoder client.
		Set the video.transcoder.url in dbp-etl.cfg to use the mock client.
		"""
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
	languageReader = LanguageReaderCreator("BLIMP").create("")
	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)

	print("Found %d filesets to process" % (len(filesets)))
	count = 0
	for inp in filesets:
		if inp.typeCode == "video":
			source = "s3://%s" % (inp.fullPath()) if inp.locationType != InputFileset.LOCAL else inp.fullPath()
			location = inp.location
			filesetPath = inp.filesetPath
			TranscodeVideo.transcodeVideoFileset(config, inp.filesetPrefix, inp.s3FileKeys(), sourceBucket=location, filesetPath=filesetPath)
			count += 1
	print("Processed %d video filesets." % (count))

# Successful tests with source on local drive
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3


# python3 load/DBPLoadController.py test s3://etl-development-input/ "ENGESVP2DV"

# time python3 load/TranscodeVideo.py test s3://etl-development-input/ "ENGESVP2DV"
