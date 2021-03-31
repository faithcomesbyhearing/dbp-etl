# AWSTranscoder.py


import time
import urllib
import json
import os
from Config import *
from LPTSExtractReader import *


class AWSTranscoder:

	result = ("{"
"'executionArn': 'arn:aws:states:us-west-2:078432969830:execution:transcode-w9gxhplj7q9mju3h:b17f2615-6480-4693-a05b-db1845bed004', "
"'files': ["
	"{'input': {'bucket': 'dbp-staging', 'duration': 519.64, 'key': 'UNRWFWP1DA/B02___01_Mark________UNRWFWP1DA.mp3'},"
	"'output': {'bitrate': 16, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 519.632, 'key': 'UNRWFWP1DA-opus16/B02___01_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 519.64, 'key': 'UNRWFWP1DA/B02___01_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 32, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 519.632, 'key': 'UNRWFWP1DA-opus32/B02___01_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 336.832, 'key': 'UNRWFWP1DA/B02___02_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 16, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 336.824, 'key': 'UNRWFWP1DA-opus16/B02___02_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 336.832, 'key': 'UNRWFWP1DA/B02___02_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 32, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 336.824, 'key': 'UNRWFWP1DA-opus32/B02___02_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 371.608, 'key': 'UNRWFWP1DA/B02___03_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 16, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 371.6, 'key': 'UNRWFWP1DA-opus16/B02___03_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 371.608, 'key': 'UNRWFWP1DA/B02___03_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 32, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 371.6, 'key': 'UNRWFWP1DA-opus32/B02___03_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, "
	"{'input': {'bucket': 'dbp-staging', 'duration': 412.216, 'key': 'UNRWFWP1DA/B02___04_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 16, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 412.208, 'key': 'UNRWFWP1DA-opus16/B02___04_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}, " 
	"{'input': {'bucket': 'dbp-staging', 'duration': 412.216, 'key': 'UNRWFWP1DA/B02___04_Mark________UNRWFWP1DA.mp3'}, 'output': {'bitrate': 32, 'bucket': 'dbp-staging', 'codec': 'opus', 'container': 'webm', 'duration': 412.208, 'key': 'UNRWFWP1DA-opus32/B02___04_Mark________UNRWFWP1DA.webm'}, 'status': 'SUCCESS'}], "
	"'output': ["
	"{'bucket': 'dbp-staging', 'container': 'webm', 'codec': 'opus', 'bitrate': 16, 'key': 'UNRWFWP1DA-opus16'}, {'bucket': 'dbp-staging', 'container': 'webm', 'codec': 'opus', 'bitrate': 32, 'key': 'UNRWFWP1DA-opus32'}"
	"], 'remaining': 0, 'status': 'SUCCESS', 'input': {'bucket': 'dbp-staging', 'key': 'UNRWFWP1DA'}, 'id': 'a9602be8-b3e2-44fa-ac45-63f22a720afc'}")


	def __init__(self, config):
		self.config = config


	def transcodeAudio(self, bucket, filesetId, filesetPath):
		url = self.config.audio_transcoder_url
		key = self.config.audio_transcoder_key
		inp = '"input": ' + self.config.audio_transcoder_input
		inp = inp.replace("$bucket", bucket)
		inp = inp.replace("$prefix", filesetPath)
		outputs = []
		for num in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
			configKey = "audio.transcoder.output." + num
			out = self.config.getOptional(configKey)
			if out != None:
				out = out.replace("$prefix", filesetPath)
				outputs.append(out)
		outputStr = '"output": [' + ", ".join(outputs) + ']'
		request = '{' + inp + ", " + outputStr + '}'
		try:
			test = json.loads(request)
		except Exception as err:
			print("json error in request:" + request)
			return None
		result = self.transcode(url, key, request)
		return result


	def transcodeVideo(self):
		print("tbd")


	def transcode(self, url, key, data):
		result = self.httpPost(url, key, data)
		print(result)
		if result == None:
			print("handle error?")
			return None
		else:
			sleepSec = self.config.audio_transcoder_sleep_sec
			jobId = result.get("id")
			print("jobId", jobId)
			status = result.get("status")
			url = url + "/" + jobId
			while status not in {"SUCCESS", "FAILED"}:
				time.sleep(10)
				result = self.httpGet(url, key)
				status = result.get("status")
			return result


	def httpPost(self, url, key, data):
		content = data.encode('utf-8')
		req = urllib.request.Request(url, data=content)
		req.add_header("Content-Type", "application/json")
		req.add_header("X-API-Key", key)
		resp = urllib.request.urlopen(req)
		out = resp.read()
		result = out.decode('utf-8')
		return json.loads(result)


	def httpGet(self, url, key):
		req = urllib.request.Request(url)
		req.add_header("Content-Type", "application/json")
		req.add_header("X-API-Key", key)
		resp = urllib.request.urlopen(req)
		out = resp.read()
		result = out.decode('utf-8')
		return json.loads(result)


	def parseResponse(self, inpFileset, response):
		outFilesets = {}

		outputs = msg.get("output", [])
		print("output", outputs)
		for output in outputs:
			print("output", output)
			bucket = "s3://" + output.get("bucket")
			print("bucket", bucket)
			container = output.get("container")
			print("container", container)
			codec = output.get("codec")
			print("codec", codec)
			bitrate = output.get("bitrate")
			print("bitrate", bitrate)
			filesetPath = output.get("key")
			print("filesetPath", filesetPath)
			filesetId = os.path.basename(filesetPath)
			print("filesetId", filesetId)
			damId = filesetId[:10]

			outFileset = InputFileset(self.config, bucket, filesetId, filesetPath, damId, "audio", 
				inpFileset.bibleId, inpFileset.index, inpFileset.lptsRecord)
			outFileset.setAudio(container, codec, bitrate)
			outFilesets[filesetPath] = outFileset

		inpFiles = {}
		for inpFile in inpFileset.files:
			inpFiles[inpFile.name] = inpFile

		files = msg.get("files", [])
		for file in files:
			inpFile = file.get("input")
			inpDuration = inpFile.get("duration")
			print("inputDuration", inpDuration)
			inpKey = inpFile.get("key")
			inpFilesetPath = os.path.dirname(inpKey)
			print("inputFilesetPath", inpFilesetPath)
			inpFilename = os.path.basename(inpKey)
			print("inputFilename", inpFilename)
			outFile = file.get("output")
			outDuration = outFile.get("duration")
			print("outputDuration", outDuration)
			outKey = outFile.get("key")
			outFilesetPath = os.path.dirname(outKey)
			outFilename = os.path.basename(outKey)
			outFilesize = 0
			outLastModified = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

			inpFile = inpFiles.get(inpFilename)
			inpFile.duration = inpDuration

			outFile = InputFile(outFilename, outFileSize, outLastModified)
			outFile.duration = outDuration
			outFileset = outFilesets.get("outputFilesetPath")
			outFileset.files.append(outfile)

		return outFilesets.values()


if (__name__ == '__main__'):
	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	inpFilesets = InputFileset.filesetCommandLineParser(config, lptsReader)
	transcoder = AWSTranscoder(config)
	#result = transcoder.transcodeAudio("dbp-staging", "UNRWFWP1DA", "UNRWFWP1DA")
	#print(result)

	inpFileset = inpFilesets[0]
	output = AWSTranscoder.result.replace("'", '"')
	msg = json.loads(output)
	trancoder.parseResponse(config, inpFileset, msg)







