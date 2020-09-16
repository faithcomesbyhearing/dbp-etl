# Transcoder.py

import time
import boto3
from S3Utility import *


class Transcoder:


	def __init__(self, config):
		region = "us-east-1"  # put in config
		self.client = boto3.client('elastictranscoder', region_name=region)
		self.audioTranscoder = '1599766493865-viabwr'
		self.preset_mp3_16bit = '1600051328798-010nb3'
		self.preset_mp3_32bit = '1600051390966-jwg0ig'
		self.preset_mp3_64bit = '1600051427984-acsojp'
		self.preset_mp3_128bit = '1351620000001-300040'



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
		return status["Job"]["Id"]


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

		elif status == "Error":
			outputs = response.get("Outputs", [])
			for output in outputs:
				objNameOut = output.get("Key")
				statusOut = output.get("Status")
				statusDetail = output.get("StatusDetail")
				print(objNameOut, statusOut, statusDetail)

		


if (__name__ == '__main__'):
	config = Config()
	s3 = S3Utility(config)
	#bucket = 'test-dbp-audio-transcode'
	#s3Key = 
	#s3.uploadFile(bucket, s3Key, filename, 'audio/mpeg')
	transcoder = Transcoder(config)
	jobId = transcoder.createMP3Job("B01___01_Matthew_____ENGESVN1DA.mp3")
	time.sleep(20)
	transcoder.getJobStatus(jobId)
	#transcoder.createMP3Job("ENGESVN2DA_B03_LUK_003_04-003_08.mp3")


"""
region = 'us-west-2'
video_bucket = 'dbp-vid'
pipeline = '1537458645466-6z62tx'  # dbp-lumo-pipe
hls_audio_video_1080p = '1556116949562-tml3vh'
hls_audio_video_720p = '1538163744878-tcmmai'
hls_audio_video_480p = '1538165037865-dri6c1'
hls_audio_video_360p = '1556118465775-ps3fba'
web_audio_video = '1351620000001-100070'
generic_1080p_audio_video = '1351620000001-000001'
generic_720p_audio_video = '1351620000001-000010'
generic_480p_audio_video = '1351620000001-000020'
generic_360p_audio_video = '1351620000001-000040'


def get_pipelines():
	transcoder_client = boto3.client('elastictranscoder', region_name=region)
	pipes = transcoder_client.list_pipelines()
	# for key in pipes['Pipelines']:
	#	 print(key)
	return pipes['Pipelines']


def get_presets():
	transcoder_client = boto3.client('elastictranscoder', region_name=region)
	presets = transcoder_client.list_presets()
	# for key in presets['Presets']:
	#	 print(key)
	return presets['Presets']


def jobs_for_path(path,video_bucket):
	job_list = []
	return_list = []
	s3_client = boto3.client('s3')
	transcoder_client = boto3.client('elastictranscoder', region_name=region)
	response = s3_client.list_objects_v2(Bucket=video_bucket, Prefix=path)
	for key in response['Contents']:
		if key['Key'][-3:] == 'mp4' and '_web' not in key['Key']:
			job = create_job(key['Key'])
			job_list.append(job)
	while len(job_list):
		for job in job_list:
			status = transcoder_client.read_job(Id=job)
			if status['Job']['Status'] == 'Complete' or status['Job']['Status'] == 'Error':
				job_list.remove(job)
				return_list.append(status)
	return return_list


def create_job(file):
	baseobj = file[:file.rfind('.')]
	#res_insert = baseobj.rfind('/')
	inputs = [
		{
			'Key': file
		}
	]
	outputs = [
		{
			'Key': baseobj + '_av720p',
			'PresetId': hls_audio_video_720p,
			'SegmentDuration': '3'
		},
		{
			'Key': baseobj + '_av480p',
			'PresetId': hls_audio_video_480p,
			'SegmentDuration': '3'
		},
		{
			'Key': baseobj + '_av360p',
			'PresetId': hls_audio_video_360p,
			'SegmentDuration': '3'
		},
		{
			'Key': baseobj + '_web.mp4',
			'PresetId': web_audio_video
		}
	]
	playlists = [
		{
			'Name': baseobj + '_stream',
			'Format': 'HLSv3',
			'OutputKeys': [
				baseobj + '_av720p',
				baseobj + '_av480p',
				baseobj + '_av360p'
			]
		}
	]
	transcoder_client = boto3.client('elastictranscoder',
									 region_name=region)
	status = transcoder_client.create_job(PipelineId=pipeline,
										  Inputs=inputs,
										  Outputs=outputs,
										  #OutputKeyPrefix='hls/',
										  Playlists=playlists)
	return status['Job']['Id']



	for item in fList:
		jList.append(create_job(item))
				
	isDone=False
	while isDone is False:
		for job in jList:
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

"""

