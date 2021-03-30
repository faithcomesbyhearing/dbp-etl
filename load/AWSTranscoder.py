# AWSTranscoder.py

from Config import *
import time
import urllib
import json

## This process should output [InputFileset]
## What should the full ouput spec be?


class AWSTranscoder:


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


	def parseResponse(self, response):
		print("tbd")
		## This method should return [InputFileset]


if (__name__ == '__main__'):
	config = Config.shared()
	transcoder = AWSTranscoder(config)
	result = transcoder.transcodeAudio("dbp-staging", "UNRWFWP1DA", "UNRWFWP1DA")
	print(result)

