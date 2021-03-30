# AWSTranscoder.py

from Config import *
#from urllib import urllib, request, parse
#import urllib.request
#import urllib.parse
import time
import urllib
import json


class AWSTranscoder:


	def __init__(self, config):
		self.config = config


	def transcodeAudio(self):
		url = "https://gig8vjo8p5.execute-api.us-west-2.amazonaws.com/job"
		key = "1b5dc5708ae8d0335afdf94e421ae5f7d772e8f13b003c9d9733bce5caf34c6a"
		data = {
  			"input": {
    		"bucket": "dbp-staging",
    		"key": "KFBMUJN2DA"
  			},
  			"output": [{
    			"bucket": "dbp-staging",
    			"key": "KFBMUJN2DA-opus16",
    			"bitrate": 16,
    			"container": "webm",
    			"codec": "opus"
  			}]
		}
		result = self.httpPost(url, key, data)
		print(result)
		if result == None:
			print("handle error?")
			return None
		else:
			jobId = result.get("id")
			print("jobId", jobId)
			status = result.get("status")
			url = url + "/" + jobId
			while status != "COMPLETE":
				time.sleep(10)  # This should be a config parameter
				result = self.httpGet(url, key)
				print(result)
				status = result.get("status")
			return result


	def transcodeVideo(self):
		print("tbd")


	def httpPost(self, url, key, data):
		content=json.dumps(data).encode('utf-8')
		#data = data.encode('utf-8')
		req = urllib.request.Request(url, data=content)
		req.add_header("Content-Type", "application/json")
		req.add_header("X-API-Key", key)
		resp = urllib.request.urlopen(req)
		out = resp.read()
		result = out.decode('utf-8')
		return json.loads(result)
		#{"id":"0b11400a-35b1-42e9-bb9f-f309bd88444c","status":"PENDING","input":{"bucket":"dbp-staging","key":"KFBMUJN2DA"},"output":[{"bucket":"dbp-staging","key":"KFBMUJN2DA-opus16","bitrate":16,"container":"webm","codec":"opus"}]}'

	def httpGet(self, url, key):
		req = urllib.request.Request(url)
		req.add_header("Content-Type", "application/json")
		req.add_header("X-API-Key", key)
		resp = urllib.request.urlopen(req)
		out = resp.read()
		result = out.decode('utf-8')
		return json.loads(result)


if (__name__ == '__main__'):
	config = Config.shared()
	transcoder = AWSTranscoder(config)
	result = transcoder.transcodeAudio()
	print(result)

