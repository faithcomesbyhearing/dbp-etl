# AWSTranscoder.py


import time
import urllib
import json
import os
from Config import *
from LanguageReader import *
from InputFileset import *
class AWSTranscoder:

	## Debug response result
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
		self.url = None
		self.key = None

	def setAudioTranscoderParameters(self):
		if hasattr(self.config, 'audio_transcoder_mock') and self.config.audio_transcoder_mock != None:
			audio_transcoder_mock = {}

			try:
				audio_transcoder_mock = json.loads(self.config.audio_transcoder_mock)
			except json.JSONDecodeError:
				# Handle the error and set a empty dict for self.audio_transcoder_mock
				audio_transcoder_mock = {}

			if audio_transcoder_mock.get("enable") == 1:
				self.url = audio_transcoder_mock.get("endpoint_url")
				self.key = audio_transcoder_mock.get("key")
				return True

		self.url = self.config.audio_transcoder_url
		self.key = self.config.audio_transcoder_key

		return True

	def transcodeAudio(self, inputFileset): 
		self.setAudioTranscoderParameters()
		inp = '"input": ' + self.config.audio_transcoder_input
		inp = inp.replace("$bucket", inputFileset.location)
		inp = inp.replace("$prefix", inputFileset.filesetPath)
		outputs = []
		# prior to July 2022, filesetPrefix was always a ten character id such as DBQWYIN2DA, which was 64kbps mp3.
		# When creating deriviate filesets from transcoding, it was correct to use the entire filesetPrefix as the base folder name and add whatever extension was necessary (eg "-opus16") 
		# A new fileset / media naming schema has been introduced that allows for many derivatives, all of which are longer than ten characters.  Using the new naming scheme, 64kbps mp3 has a 
		# fileset DBQWYIN2DA-mp3-64. The ten character fileset id is deprecated, but there are no current plans to remove it.
		# With the introduction of 128kbps as the golden copy, the input fileset id will be DBQWYIN2DA-mp3-64. So we can no longer just append to the end of the input fileset; we need to use only the first 
		# ten characters.
		#inputFileset.filesetPrefix is of the form "audio/DBQ015/DBQWYIN2DA-mp3-128". We need "audio/DBQ015/DBQWYIN2DA"
		lastSlash = inputFileset.filesetPrefix.rindex("/")
		outFilesetBase = inputFileset.filesetPrefix[:lastSlash+11]

		# audio.transcoder.output.0 = { "bucket": "${S3_BUCKET}", "key": "\$prefix-opus16", "bitrate": 16, "container": "webm", "codec": "opus" }
		# audio.transcoder.output.1 = { "bucket": "${S3_BUCKET}", "key": "\$prefix-mp3-64", "bitrate": 64, "container": "mp3", "codec": "mp3" }

		for num in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
			configKey = "audio.transcoder.output." + num
			out = self.config.getOptional(configKey)
			if out != None:
				out = out.replace("$prefix", outFilesetBase)
				print("AWSTranscoder:transcodeAudio... new output format: %s" %(out))
				outputs.append(out)
				print("AWSTranscoder:transcodeAudio... after adding new output format. outputs size is : %d" %(len(outputs)))
		outputStr = '"output": [' + ", ".join(outputs) + ']'
		request = '{' + inp + ", " + outputStr + '}'
		print("AWSTranscoder:transcodeAudio... request to be sent to lambda:%s" %(request))
		try:
			test = json.loads(request)
		except Exception as err:
			print("ERROR: json error in request:" + request)
			return []
		result = self.transcode(self.url, self.key, request)
		outFilesets = self.parseAudioResponse(inputFileset, result)
		for fileset in outFilesets:
			print("AWSTranscoder:transcodeAudio... outfileset: %s" %(fileset))
			fileset.setFileSizes()
		return outFilesets


	def transcodeVideo(self):
		print("tbd")


	def transcode(self, url, key, data):
		print("AWSTranscoder:transcode....url: %s, key: %s, request: %s" % (url, key, data))
		result = self.httpPost(url, key, data)
		#print(result)
		if result == None:
			print("ERROR: No response from transcode job request", data)
			return None
		else:
			sleepSec = self.config.audio_transcoder_sleep_sec
			jobId = result.get("id")
			print("AWSTranscoder jobId %s for %s" % (jobId, data))
			status = result.get("status")
			url = url + "/" + jobId
			while status == "PENDING":
				time.sleep(30)
				result = self.httpGet(url, key)
				#print(result)
				status = result.get("status")
				if status == "FAILED":
					raise Exception("Transcoding Job Failed: %s" %(result))

			print("AWSTranscoder:transcode. result from the transcoder lambda: %s" %(result))
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
		for i in range(5):
			try:
				resp = urllib.request.urlopen(req)
				out = resp.read()
				result = out.decode('utf-8')
				return json.loads(result)
			except urllib.error.HTTPError as e:
				if i >= 4:
					raise
				time.sleep(10)


	def parseAudioResponse(self, inpFileset, response):
		print("AWSTranscoder:parseAudioResponse. inpFileset: %s, response: %s" %(inpFileset, response))
		outFilesets = {}

		outputs = response.get("output", [])
		print("AWSTranscoder:parseAudioResponse. count of outputs: %d" %(len(outputs)))
		for output in outputs:
			bucket = "s3://" + output.get("bucket")
			filesetPath = output.get("key")
			filesetId = os.path.basename(filesetPath)
			damId = filesetId[:10]
			container = output.get("container")
			codec = output.get("codec")
			bitrate = output.get("bitrate")

			outFileset = InputFileset(self.config, bucket, filesetId, filesetPath, damId, "audio", 
				inpFileset.bibleId, inpFileset.index, inpFileset.languageRecord)
			outFileset.files = [] # Erase files, because they will be overwritten by transcoding
			outFileset.setAudio(container, codec, bitrate)
			outFilesets[filesetPath] = outFileset

		files = response.get("files", [])
		print("AWSTranscoder:parseAudioResponse. count of files: %d" %(len(files)))
		for file in files:
			inpJson = file.get("input")
			inpDuration = inpJson.get("duration")
			inpKey = inpJson.get("key")
			inpFilename = os.path.basename(inpKey)

			inpFile = inpFileset.getInputFile(inpFilename)
			if inpFile != None:
				inpFile.duration = inpDuration

			outJson = file.get("output")
			outDuration = outJson.get("duration")
			outKey = outJson.get("key")
			outFilesetPath = os.path.dirname(outKey)
			outFilename = os.path.basename(outKey)
			outFileSize = 0
			outLastModified = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

			outFile = InputFile(outFilename, outFileSize, outLastModified)
			outFile.duration = outDuration
			outFileset = outFilesets.get(outFilesetPath)
			outFileset.files.append(outFile)

		return outFilesets.values()


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from InputProcessor import InputProcessor

	config = Config.shared()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	inpFilesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
	inpFileset = inpFilesets[0]
	transcoder = AWSTranscoder(config)
	outFilesets = transcoder.transcodeAudio(inpFileset)

	#output = AWSTranscoder.result.replace("'", '"')
	#msg = json.loads(output)
	#outFilesets = transcoder.parseAudioResponse(inpFileset, msg)
	#print("INPUT:")
	#print(inpFileset.toString())
	#print("OUTPUT:")
	for outFileset in outFilesets:
		print(outFileset.toString())



# time python3 load/AWSTranscoder.py test-video s3://dbp-staging UNRWFWP1DA







