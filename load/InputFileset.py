# InputFileset.py

# This class carries information about the fileset being processed through the
# dbp-etl program which passes an array of these.

# There are 5 folders: validate, upload, transcode, database, complete.
# As a fileset properly completes one step it is moved to the next folder
# So, that when that next step is performed only those that completed the
# prior step are going to be processed.

import os
import json
from datetime import datetime
from Log import *
from Config import *
from LPTSExtractReader import *
from PreValidate import *

class InputFile:

	def __init__(self, name, size, lastModified):
		self.name = name
		self.size = size
		self.lastModified = lastModified
		self.duration = None

	def filenameTuple(self):
		return (self.name, self.size, self.lastModified)

	def toString(self):
		results = []
		results.append("name=%s" % (self.name,))
		results.append("size=%d" % (self.size,))
		results.append("date=%s" % (self.lastModified))
		if self.duration != None:
			results.append("duration={:.{}f}".format(self.duration, 3))
		return " ".join(results)


class InputFileset:

	BUCKET = "BUCKET"
	LOCAL = "LOCAL"

	## These arrays are the 5 stages that an InputFileset must pass through
	validate = []
	upload = []
	transcode = []
	database = []
	complete = []


	## parse command line, and return [InputFileset]
	def filesetCommandLineParser(config, lptsReader):
		if len(sys.argv) < 4:
			print("FATAL command line parameters: config_profile  s3://bucket|localPath  filesetPath_list ")
			sys.exit()

		results = []
		location = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
		filesetPaths = sys.argv[3:]
		preValidate = PreValidate(lptsReader)

		for filesetPath in filesetPaths:
			filesetPath = filesetPath[:-1] if filesetPath.endswith("/") else filesetPath
			filesetId = filesetPath.split("/")[-1]
			lptsData = preValidate.validateFilesetId(filesetId)
			if lptsData != None:
				(lptsRecord, damId, typeCode, bibleId, index) = lptsData
				filesetId = filesetId[:6] if typeCode == "text" else filesetId
				preValidate.validateLPTS(typeCode, filesetId, lptsRecord, index)
				if not preValidate.hasErrors(filesetId):
					results.append(InputFileset(config, location, filesetId, filesetPath, damId, typeCode, bibleId, index, lptsRecord))
		Log.addPreValidationErrors(preValidate.messages)
		return results


	def __init__(self, config, location, filesetId, filesetPath, damId, typeCode, bibleId, index, lptsRecord):
		self.config = config
		if location.startswith("s3://"):
			self.locationType = InputFileset.BUCKET
			self.location = location[5:]
		else:
			self.locationType = InputFileset.LOCAL
			self.location = location
		self.filesetId = filesetId
		self.filesetPath = filesetPath
		self.lptsDamId = damId
		self.typeCode = typeCode
		self.bibleId = bibleId
		self.index = index
		self.lptsRecord = lptsRecord
		self.filesetPrefix = "%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId)
		self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, self.filesetId)
		self.databasePath = "%s%s.db" % (config.directory_accepted, self.filesetId) if self.typeCode == "text" else None
		self.files = self._setFilenames()
		self.filesMap = None
		self.mediaContainer = None
		self.mediaCodec = None
		self.mediaBitrate = None


	def toString(self):
		results = []
		results.append("InputFileset\n")
		results.append("location=" + self.location + "\n")
		results.append("locationType=" + self.locationType)
		results.append(" filesetId=" + self.filesetId)
		results.append(" lptsDamId=" + self.lptsDamId)
		results.append(" stockNum=" + self.stockNum())
		results.append(" typeCode=" + self.typeCode)
		results.append(" bibleId=" + self.bibleId)
		results.append(" index=" + str(self.index) + "\n")
		results.append("filesetPrefix=" + self.filesetPrefix + "\n")
		results.append("csvFilename=" + self.csvFilename + "\n")
		for file in self.files:
			results.append(file.toString() + "\n")
		return " ".join(results)


	def _setFilenames(self):
		results = []
		ignoreSet = {"Thumbs.db"}
		if self.locationType == InputFileset.LOCAL:
			pathname = self.fullPath()
			if os.path.isdir(pathname):
				for filename in [f for f in os.listdir(pathname) if not f.startswith('.')]:
					if filename not in ignoreSet and os.path.isfile(pathname + os.sep + filename):
						filepath = pathname + os.sep + filename
						filesize = os.path.getsize(filepath)
						modifiedTS = os.path.getmtime(filepath)
						lastModified = str(datetime.fromtimestamp(modifiedTS)).split(".")[0]
						results.append(InputFile(filename, filesize, lastModified))
			else:
				Log.getLogger(self.filesetId).message(Log.EROR, "Invalid pathname %s" % (pathname))
		else:
			request = { 'Bucket': self.location, 'MaxKeys': 1000, 'Prefix': self.filesetPath + "/" }
			hasMore = True
			while hasMore:
				response = self.config.s3_client.list_objects_v2(**request)
				for item in response.get('Contents', []):
					objKey = item.get('Key')
					filename = objKey[len(self.filesetPath) + 1:]
					size = item.get('Size')
					lastModified = str(item.get('LastModified'))
					lastModified = lastModified.split("+")[0]
					results.append(InputFile(filename, size, lastModified))
				hasMore = response['IsTruncated']
				if hasMore:
					request['ContinuationToken'] = response['NextContinuationToken']
			if len(results) == 0:
				Log.getLogger(self.filesetId).message(Log.EROR, "Invalid bucket %s or prefix %s" % (self.location, self.filesetPath))
		return results


	def setFileSizes(self):
		request = { 'Bucket': self.location, 'MaxKeys': 1000, 'Prefix': self.filesetPrefix + "/" }
		response = self.config.s3_client.list_objects_v2(**request)
		contents = response['Contents']
		for item in contents:
			key = item['Key']
			filename = os.path.basename(key)
			file = self.getInputFile(filename)
			if file != None:
				file.size = item['Size']


	def getInputFile(self, name):
		if self.filesMap == None:
			self.filesMap = {}
			for file in self.files:
				self.filesMap[file.name] = file
		return self.filesMap.get(name)


	def setAudio(self, container, codec, bitrate):
		self.mediaContainer = container
		self.mediaCodec = codec
		self.mediaBitrate = bitrate


	def stockNum(self):
		return self.lptsRecord.Reg_StockNumber()


	def fullPath(self):
		return self.location + os.sep + self.filesetPath


	def filenames(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "xml", "jpg", "tif" }:  ### Does this belong in _setFilenames
				results.append(file.name)
		return results


	def filenamesTuple(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "xml", "jpg", "tif" }:
				results.append(file.filenameTuple())
		return results


	def s3FileKeys(self):
		results = []
		for file in self.files:
			if not file.name.endswith(".xml"):
				objectKey = self.filesetPrefix + "/" + file.name
				results.append(objectKey)
		return results


	def downloadFiles(self):
		directory = self.config.directory_upload_aws + self.filesetPath
		if not os.path.isdir(directory):
			os.makedirs(directory)
		else:
			for f in os.listdir(directory):
				os.remove(os.path.join(directory, f))
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "jpg", "tif" }:  ## notice that I do pick up the xml file
				objectKey = self.filesetPath + "/" + file.name
				filepath = directory + os.sep + file.name
				try:
					print("Download s3://%s/%s to %s" % (self.location, objectKey, filepath))
					self.config.s3_client.download_file(self.location, objectKey, filepath)
				except Exception as err:
					print("ERROR: Download s3://%s/%s failed with error %s" % (self.location, objectKey, err))
		return directory



if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	InputFileset.validate = InputFileset.filesetCommandLineParser(config, lptsReader)
	for inp in InputFileset.validate:
		if inp.typeCode == "text" and inp.locationType == InputFileset.BUCKET:
			inp.downloadFiles()
		print(inp.toString())
	Log.writeLog(config)

# python3 load/InputFileset.py test /Volumes/FCBH/files/complete/audio/ENGESV/ ENGESVN2DA ENGESVN2DA16

# python3 load/InputFileset.py newdata s3://dbp-prod ENGESVN2DA ENGESVN2DA16

# time python3 load/InputFileset.py test s3://test-dbp-etl HYWWAVN2ET

