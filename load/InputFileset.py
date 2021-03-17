# InputFileset.py

# This class carries information about the fileset being processed through the
# dbp-etl program which passes an array of these.

# There are 5 folders: validate, upload, transcode, database, complete.
# As a fileset properly completes one step it is moved to the next folder
# So, that when that next step is performed only those that completed the
# prior step are going to be processed.

import os
import json
from Log import *
from Config import *
from LPTSExtractReader import *
from PreValidate import *


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
	def commandLineFactory(config, lptsReader, preValidate):
		if len(sys.argv) < 4:
			print("FATAL command line parameters: config_profile  s3://bucket|localPath  filesetId_list ")
			sys.exit()

		location = sys.argv[2]
		filesetIds = sys.argv[3:]

		for filesetId in filesetIds:
			lptsData = preValidate.validateFilesetId(filesetId)
			if lptsData != None:
				(stockNo, damId, typeCode, bibleId, index) = lptsData
				if typeCode == "text":
					filesetId = filesetId[:6]
				preValidate.validateLPTS(typeCode, bibleId, filesetId)
				if not preValidate.hasErrors(filesetId):
					InputFileset.validate.append(InputFileset(config, location, filesetId, stockNo, damId, typeCode, bibleId, index))
		Log.addPreValidationErrors(preValidate.messages)


	def __init__(self, config, location, filesetId, stockNum, damId, typeCode, bibleId, index):
		if location.startswith("s3://"):
			self.locationType = InputFileset.BUCKET
			self.location = location[5:]
		else:
			self.locationType = InputFileset.LOCAL
			self.location = location
		self.filesetId = filesetId
		self.lptsDamId = damId
		self.stockNum = stockNum
		self.typeCode = typeCode
		self.bibleId = bibleId
		self.index = index
		self.filesetPrefix = "%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId)
		self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, self.filesetId)
		self.database = "%s%s.db" % (config.directory_accepted, self.filesetId) if self.typeCode == "text" else None
		self.files = self._setFilenames(config)

	def toString(self):
		results = []
		results.append("InputFileset\n")
		results.append("location=" + self.location + "\n")
		results.append("locationType=" + self.locationType)
		results.append(" filesetId=" + self.filesetId)
		results.append(" lptsDamId=" + self.lptsDamId)
		results.append(" stockNum=" + self.stockNum)
		results.append(" typeCode=" + self.typeCode)
		results.append(" bibleId=" + self.bibleId)
		results.append(" index=" + str(self.index) + "\n")
		results.append("filesetPrefix=" + self.filesetPrefix + "\n")
		results.append("csvFilename=" + self.csvFilename + "\n")
		results.append("files=" + ", ".join(self.files))
		return " ".join(results)


	def _setFilenames(self, config):
		results = []
		ignoreSet = {"Thumbs.db"}
		if self.locationType == InputFileset.LOCAL:
			pathname = self.location + self.filesetId
			if os.path.isdir(pathname):
				for filename in [f for f in os.listdir(pathname) if not f.startswith('.')]:
					if not ignoreSet.has(filename):
						results.append(filename)
			else:
				Log.getLogger(self.filesetId).message(Log.EROR, "Invalid pathname %s" % (pathname))
		else:
			print("search for ", self.filesetPrefix)
			request = { 'Bucket': self.location, 'MaxKeys': 1000, 'Prefix': self.filesetPrefix }
			hasMore = True
			while hasMore:
				response = config.s3_client.list_objects_v2(**request)
				for item in response.get('Contents', []):
					objKey = item.get('Key')
					parts = objKey.split("/")
					results.append(parts[-1])
				hasMore = response['IsTruncated']
				if hasMore:
					request['ContinuationToken'] = response['NextContinuationToken']
			if len(results) == 0:
				Log.getLogger(self.filesetId).message(Log.EROR, "Invalid bucket %s or prefix %s" % (self.location, self.filesetPrefix))
		return results

	def fullPath(self):
		return self.location + self.filesetId


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	preValidate = PreValidate(lptsReader)
	InputFileset.commandLineFactory(config, lptsReader, preValidate)
	for inp in InputFileset.validate:
		print(inp.toString())
	Log.writeLog(config)

# python3 load/InputFileset.py test /Volumes/FCBH/files/complete/audio/ENGESV/ ENGESVN2DA ENGESVN2DA16

# python3 load/InputFileset.py newdata s3://dbp-prod ENGESVN2DA ENGESVN2DA16

