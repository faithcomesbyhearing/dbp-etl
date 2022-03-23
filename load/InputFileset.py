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
from RunStatus import *
from LPTSExtractReader import *
from SqliteUtility import *
from PreValidate import *
from AWSSession import *
from UnicodeScript import *

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
		results.append("date=%s" % (self.lastModified)) ## I think this should be deprecated
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

	def parseFilesetId(directory) :
		return directory[:7] + "_" + directory[8:] + "-usx" if directory[8:10] == "ET" else directory

	## parse command line, and return [InputFileset]
	def filesetCommandLineParser(config, s3Client, lptsReader):
		if len(sys.argv) < 4:
			print("FATAL command line parameters: config_profile  s3://bucket|localPath  filesetPath_list ")
			sys.exit()

		results = []
		location = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
		filesetPaths = sys.argv[3:]

		unicodeScript = UnicodeScript()
		preValidate = PreValidate(lptsReader, unicodeScript, s3Client, location)

		for filesetPath in filesetPaths:
			stocknumbers = preValidate.getStockNumbersFromFile(filesetPath, location.replace("s3://", ""))
			hasStocknumbersFile = True if len(stocknumbers) > 0 else False
			filesetPath = filesetPath[:-1] if filesetPath.endswith("/") else filesetPath
			directory = filesetPath.split("/")[-1]
			filesetId = directory

			# the variable filesetId refers to a directory name. Originally, the directory name was an actual fileset id. Now,
			# the directory name can be either (a) a fileset id; or (b) the name of a directory which contains a file called
			# stocknumber.txt, which contains one or more stocknumbers associated with the content in that directory
			if hasStocknumbersFile == False:
				filesetId = InputFileset.parseFilesetId(directory)

			(dataList, messages) = PreValidate.validateDBPELT(lptsReader, s3Client, location, filesetId, filesetPath)
			for data in dataList:
				inp = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
					data.typeCode, data.bibleId(), data.index, data.lptsRecord, data.fileList)
				#print("INPUT", inp.toString())
				results.append(inp)
			if messages != None and len(messages) > 0:
				RunStatus.set(filesetId, False)
				Log.addPreValidationErrors(messages)
			if (dataList == None or len(dataList) == 0) and (messages == None or len(messages) == 0):
				Log.getLogger(filesetPath).message(Log.FATAL, "Unknown Error in InputFileset.filesetCommandLineParser()")
				Log.writeLog(config)
				sys.exit()
		return results


	def __init__(self, config, location, filesetId, filesetPath, damId, typeCode, bibleId, index, lptsRecord, fileList = None):
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
		if self.typeCode == "text":
			self.databasePath = "%s%s.db" % (config.directory_accepted, damId[:7] + "_" + damId[8:])
		else:
			self.databasePath = None
		if self.typeCode == "text" and len(self.filesetId) < 10:
			self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, damId[:7] + "_" + damId[8:])
		else:
			self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, self.filesetId)
		if fileList != None:
			self.files = self._downloadSelectedFiles(fileList)
		else:
			self.files = self._setFilenames()
		self.filesMap = None
		self.mediaContainer = None
		self.mediaCodec = None
		self.mediaBitrate = None
		RunStatus.add(self)


	def toString(self):
		results = []
		results.append("InputFileset\n")
		results.append("location=" + self.location + "\n")
		results.append("locationType=" + self.locationType)
		results.append("prefix=%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId))
		results.append(" damId=" + self.lptsDamId)
		results.append(" stockNum=" + self.stockNum())
		results.append(" index=" + str(self.index))
		results.append(" script=" + str(self.lptsRecord.Orthography(self.index)) + "\n")
		results.append("filesetPrefix=" + self.filesetPrefix + "\n")
		results.append("csvFilename=" + self.csvFilename + "\n")
		for file in self.files:
			results.append(file.toString() + "\n")
		return " ".join(results)


	## This method is used instead of _setFilenames when a specific list of files has been provided
	def _downloadSelectedFiles(self, fileList):
		results = []
		s3Client = AWSSession.shared().s3Client
		directory = self.config.directory_upload_aws + self.filesetId
		if not os.path.isdir(directory):
			os.makedirs(directory)
		else:
			for f in os.listdir(directory):
				os.remove(os.path.join(directory, f))
		for filename in fileList:
			objectKey = self.filesetPath + "/" + filename
			filepath = directory + os.sep + filename
			try:
				print("Download s3://%s/%s to %s" % (self.location, objectKey, filepath))
				s3Client.download_file(self.location, objectKey, filepath)
				filesize = os.path.getsize(filepath)
				modifiedTS = os.path.getmtime(filepath)
				lastModified = str(datetime.fromtimestamp(modifiedTS)).split(".")[0]
				results.append(InputFile(filename, filesize, lastModified))				
			except Exception as err:
				print("ERROR: Download s3://%s/%s failed with error %s" % (self.location, objectKey, err))
		self.locationType = InputFileset.LOCAL
		self.location = self.config.directory_upload_aws
		self.filesetPath = self.filesetId		
		return results


	def _setFilenames(self):
		results = []
		ignoreSet = {"Thumbs.db"}
		### Future NOTE: If typeCode == text and subTypeCode in {text_html, text_format}
		### Get the filenames by a select from self.databasePath
		### This must be coded when we generate text_html or text_format filesets
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
				response = AWSSession.shared().s3Client.list_objects_v2(**request)
				for item in response.get('Contents', []):
					objKey = item.get('Key')
					filename = objKey[len(self.filesetPath) + 1:]
					if not filename.startswith("."):
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


	def addInputFile(self, filename, filesize):
		self.files.append(InputFile(filename, filesize, None))


	def setFileSizes(self):
		request = { 'Bucket': self.location, 'MaxKeys': 1000, 'Prefix': self.filesetPrefix + "/" }
		response = AWSSession.shared().s3Client.list_objects_v2(**request)
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


	def locationForS3(self):
		if self.locationType == InputFileset.BUCKET:
			return "s3://" + self.location
		else:
			return self.location


	def fullPath(self):
		if self.locationType == InputFileset.LOCAL:
			return self.location + os.sep + self.filesetPath
		else:
			return self.location + "/" + self.filesetPath


	def subTypeCode(self):
		if self.typeCode == "text":
			if self.filesetId.endswith("-usx"):
				return "text_usx"
			elif self.filesetId.endswith("-html"):
				return "text_html"
			else:
				return "text_format"
		else:
			return None


	def isMP3Fileset(self):
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext == "mp3":
				return True
		return False


	def filenames(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "xml", "jpg", "tif", "png", "zip" }:  ### Does this belong in _setFilenames
				results.append(file.name)
		return results


	def filenamesTuple(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "xml", "jpg", "tif", "png", "zip" }:
				results.append(file.filenameTuple())
		return results


	def s3FileKeys(self):
		results = []
		for file in self.files:
			if not file.name.endswith(".xml"):
				objectKey = self.filesetPrefix + "/" + file.name
				results.append(objectKey)
		return results


	def artFiles(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext in { "jpg", "tif", "png" }:
				results.append(file.name)
		return results


	def zipFile(self):
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext == "zip":
				return file
		return None


	## This method is used to download files to local disk when needed for processing, such as BiblePublisher
	def downloadFiles(self):
		directory = self.config.directory_upload_aws + self.filesetPath
		if not os.path.isdir(directory):
			os.makedirs(directory)
		else:
			for f in os.listdir(directory):
				os.remove(os.path.join(directory, f))
		for file in self.files:
			ext = file.name.split(".")[-1]
			if not ext in { "jpg", "tif", "png", "zip" }:  ## notice that I do pick up the xml file
				objectKey = self.filesetPath + "/" + file.name
				filepath = directory + os.sep + file.name
				try:
					print("Download s3://%s/%s to %s" % (self.location, objectKey, filepath))
					AWSSession.shared().s3Client.download_file(self.location, objectKey, filepath)
				except Exception as err:
					print("ERROR: Download s3://%s/%s failed with error %s" % (self.location, objectKey, err))
		self.locationType = InputFileset.LOCAL
		self.location = self.config.directory_upload_aws		
		return directory


	def numberUSXFileset(self, processedFileset):
		if len(self.files[0].name) < 9:
			if self.locationType == InputFileset.LOCAL:
				directory = self.fullPath() + os.sep
			else:
				directory = self.config.directory_upload_aws + self.filesetPath + os.sep # download path
			bibleDB = SqliteUtility(processedFileset.databasePath)
			resultList = bibleDB.selectList("SELECT code FROM tableContents ORDER BY rowId", ())
			for index in range(0, len(resultList)):
				num = index + 1
				if num < 10:
					pos = "00" + str(num)
				elif num < 100:
					pos = "0" + str(num)
				else:
					pos = str(num)
				filename = resultList[index] + ".usx"
				newFilename = pos + filename
				os.rename(directory + filename, directory + newFilename)
			self.files = self._setFilenames() # reload files after renaming


	def batchName(self):
		if self.typeCode == "text" and len(self.filesetId) < 10:
			return self.lptsDamId[:7] + "_" + self.lptsDamId[8:]
		else:
			return self.filesetId


if (__name__ == '__main__'):
	config = Config()
	s3Client = AWSSession.shared().s3Client
	session = boto3.Session(profile_name = config.s3_aws_profile)
	s3Client = session.client('s3')
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	InputFileset.validate = InputFileset.filesetCommandLineParser(config, s3Client, lptsReader)
	for inp in InputFileset.validate:
		if inp.typeCode == "text" and inp.locationType == InputFileset.BUCKET:
			inp.downloadFiles()

			if os.path.isfile(inp.databasePath):
				os.remove(inp.databasePath)
			bibleDB = SqliteUtility(inp.databasePath)
			print("DatabasePath", inp.databasePath)
			bibleDB.execute("CREATE TABLE tableContents (code text not null)", ())
			for file in inp.files:
				if file.name.endswith(".usx"):
					bibleDB.execute("INSERT INTO tableContents (code) VALUES (?)", (file.name.split(".")[0],))
			inp.numberUSXFileset(inp)
		#print(inp.toString())
		#print("subtype", inp.subTypeCode())
	Log.writeLog(config)

# python3 load/InputFileset.py test s3://etl-development-input Spanish_N2SPNTLA_USX

# python3 load/InputFileset.py test /Volumes/FCBH/files/complete/audio/ENGESV/ ENGESVN2DA ENGESVN2DA16

# python3 load/InputFileset.py newdata s3://dbp-prod ENGESVN2DA ENGESVN2DA16

# time python3 load/InputFileset.py test s3://test-dbp-etl HYWWAVN2ET


# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Abidji N2ABIWBT/05 DBP & GBA/Abidji_N2ABIWBT/Abidji_N2ABIWBT_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Acholi N2ACHBSU/05 DBP & GBA/Acholi_N2ACHBSU - Update/Acholi_N2ACHBSU_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Achuar [ACU]/N2ACUTBL/05 DBP & GBA/Achuar_N2ACUTBL - Update/Achuar_N2ACUTBL_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Agni Sanvi N2ANYWBT/05 DBP & GBA/Agni Sanvi_N2ANYWBT/Agni Sanvi_N2ANYWBT_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Agta, Pahanan N2APFCMU/05 DBP & GBA/Agta, Pahanan_N2APFCMU/Agta, Pahanan_N2APFCMU_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Akan [AKA]/N1AKABIB (Asante)/05 DBP & GBA/Akan, Asante_N1AKABIB/Akan, Asante_N1AKABIB_USX"







