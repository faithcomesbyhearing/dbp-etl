# InputFileset.py

# This class carries information about the fileset being processed through the
# dbp-etl program which passes an array of these.

# There are 5 folders: validate, upload, transcode, database, complete.
# As a fileset properly completes one step it is moved to the next folder
# So, that when that next step is performed only those that completed the
# prior step are going to be processed.

import os
from datetime import datetime
from Log import *
from Config import *
from RunStatus import *
from LanguageReader import *
from SqliteUtility import *
from PreValidate import *
from AWSSession import *
from LanguageReader import LanguageRecordInterface

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

	def startswith(self, text):
		return self.name.startswith(text)

	def isZip(self):
		return self.name.endswith(".zip")

	def zipBookId(self):
		if self.isZip():
			parts = self.name.split("_")
			return parts[1][0:3] if len(parts) > 1 else None

		return None
	def hasValidFilesetPath(self, typeCode, bibleId, filesetId):
		directory = os.path.dirname(self.name)
		directoryParts = directory.split('/')

		if len(directoryParts) < 3:
			return False

		return directoryParts[0] == typeCode and directoryParts[1] == bibleId and directoryParts[2] == filesetId

	def extension(self):
		parts = self.name.split(".")
		if len(parts) > 1:
			return parts[-1]
		else:
			return None

	def only_file_name(self) -> str:
		"""
		Return just the filename portion of self.name, stripping any trailing slashes.
		"""
		# Remove any trailing slash or backslash
		cleaned = self.name.rstrip("/\\")
		return os.path.basename(cleaned)

class InputFileset:

	BUCKET = "BUCKET"
	LOCAL = "LOCAL"

	## These arrays are the 5 stages that an InputFileset must pass through
	validate = []
	upload = []
	transcode = []
	database = []
	complete = []

	

	def __init__(self, config, location, filesetId, filesetPath, damId, typeCode, bibleId, index, languageRecord, fileList = None):
		print("InputFileset construction. filesetId: %s location: %s" % (filesetId, location))
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
		self.languageRecord = languageRecord
		if self.typeCode == "text":
			self.databasePath = "%s%s.db" % (config.directory_accepted, self.textLptsDamId())
			# if filesetId is of type text and it formatted as a stocknumber, we can replaced 2 or 1 peer "_".
			self.filesetId = self.textFilesetId()
		else:
			self.databasePath = None
		if self.typeCode == "text" and len(self.filesetId) < 10:
			# BWF. if we encounter this error message, go upstream and change filesetId to 10 chars
			print("*** !!! text fileset with less than 10 characters !!! ***")
			self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, self.textLptsDamId())
		else:
			self.csvFilename = "%s%s_%s_%s.csv" % (config.directory_accepted, self.typeCode, self.bibleId, self.filesetId)
		if fileList != None:
			self.files = self._downloadSelectedFiles(fileList)
		else:
			self.files = self._setFilenames()
		self.filesetPrefix = "%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId)

		self.filesMap = None
		self.mediaContainer = None
		self.mediaCodec = None
		self.mediaBitrate = None
		RunStatus.add(self)


	def toString(self):
		results = []
		results.append("InputFileset\n")
		results.append("location=" + self.location + "\n")
		results.append("filesetPath=" + self.filesetPath + "\n")
		results.append("fullPath=" + self.fullPath() + "\n")
		results.append("locationType=" + self.locationType)
		results.append("prefix=%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId))
		results.append(" damId=" + self.lptsDamId)
		results.append(" stockNum=" + self.stockNum())
		results.append(" index=" + str(self.index))
		results.append(" subTypeCode=" + str(self.subTypeCode()))
		results.append(" script=" + str(self.languageRecord.Orthography(self.index)) + "\n")
		results.append("filesetPrefix=" + self.filesetPrefix + "\n")
		results.append("csvFilename=" + self.csvFilename + "\n")
		for file in self.files:
			results.append(file.toString() + "\n")
		return " ".join(results)

	def getSetTypeCode(self):
		if self.typeCode == "video":
			return "video_stream"
		elif self.typeCode == "audio":
			code = self.filesetId[7:9]
			if code == "1D":
				return "audio"
			elif code == "2D":
				return "audio_drama"
			else:
				code = self.filesetId[8:10]
				if code == "1D":
					return "audio"
				elif code == "2D":
					return "audio_drama"
				else:
					print("ERROR: file type not known for %s, set_type_code set to 'unknown'" % (self.filesetId))
					sys.exit()
		## This method only works for audio and video filesets
		else:
			print("ERROR: type code %s is not know for %s" % (self.typeCode, self.filesetId))
			sys.exit()

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
				# print("Download s3://%s/%s to %s" % (self.location, objectKey, filepath))
				s3Client.download_file(self.location, objectKey, filepath)
				filesize = os.path.getsize(filepath)
				modifiedTS = os.path.getmtime(filepath)
				lastModified = str(datetime.fromtimestamp(modifiedTS)).split(".")[0]
				if filename != 'stocknumber.txt':
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
						if filename != 'stocknumber.txt':
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
					if not self.ignoreFilename(filename):
						size = item.get('Size')
						lastModified = str(item.get('LastModified'))
						lastModified = lastModified.split("+")[0]
						if filename != 'stocknumber.txt':
							results.append(InputFile(filename, size, lastModified))
				hasMore = response['IsTruncated']
				if hasMore:
					request['ContinuationToken'] = response['NextContinuationToken']
			if len(results) == 0:
				Log.getLogger(self.filesetId).message(Log.EROR, "Invalid bucket %s or prefix %s" % (self.location, self.filesetPath))
		return results

	def ignoreFilename(self, filename):
		return filename.startswith(".") or len(filename) == 0

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
		return self.languageRecord.Reg_StockNumber()


	def locationForS3(self):
		if self.locationType == InputFileset.BUCKET:
			return "s3://" + self.location
		else:
			return self.location

	def textFilesetId(self):
		return LanguageRecordInterface.transformToTextId(self.filesetId)

	def textLptsDamId(self):
		return LanguageRecordInterface.transformToTextId(self.lptsDamId)

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
			elif self.filesetId.endswith("-json"):
				return "text_json"
			else:
				return "text_plain"
		else:
			return None

	def isDerivedFileset(self):
		if self.typeCode == "text":
			if self.filesetId.endswith("-usx") or self.filesetId.endswith("-html") or self.filesetId.endswith("-json"):
				return True
		elif self.typeCode == "audio" and self.filesetId.endswith("-opus16"):
				return True

		return False

	def isMP3Fileset(self):
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext == "mp3":
				return True
		return False

	def isMP4Fileset(self):
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext == "mp4":
				return True
		return False

	def videoFileNamesByBook(self, bookId):
		results = []
		for file in self.files:
			if bookId in file.name:
				results.append(file.name)
		return results

	def hasGospelAndActFilmsVideo(self, books_allowed):
		for book_id in books_allowed:
			if self.videoFileNamesByBook(book_id):
				return True
		return False

	def videoCovenantFileNames(self):
		results = []
		for file in self.files:
			if "covenant" in file.name.lower() and file.name.endswith(".mp4"):
				results.append(file.name)
		return results

	def audioFileNames(self):
		results = []
		for file in self.files:
			if file.name.endswith(".mp3") and "opus16" not in file.name:
				results.append(file.name)
		return results

	def filenames(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext not in { "xml", "jpg", "tif", "png", "zip", "pdf" }:  ### Does this belong in _setFilenames
				results.append(file.name)
		return results


	def filenamesTuple(self):
		results = []
		for file in self.files:
			ext = file.name.split(".")[-1]
			if ext not in { "xml", "jpg", "tif", "png", "zip", "txt", "pdf" }:
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
			if (file.name.startswith("gf")):
				continue
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

	def zipFilesIndexedByBookId(self):
		results = {}

		for file in self.files:
			if file.isZip() and file.zipBookId() is not None:
				results[file.zipBookId()] = file
		return results
	
	def thumbnailFiles(self):
		results = []
		for file in self.files:
			if (file.name.startswith("gf")):
				ext = file.name.split(".")[-1]
				if ext in { "jpg", "tif", "png" }:
					results.append(file.name)
		return results

	def getListFilesFromBucket(self, location, filesetPath, lenFilesetPatch, invokedBy = "General"):
		resultList = []
		request = { 'Bucket': location, 'MaxKeys': 1000, 'Prefix': filesetPath }
		hasMore = True
		while hasMore:
			response = AWSSession.shared().s3Client.list_objects_v2(**request)
			for item in response.get('Contents', []):
				objKey = item.get('Key')
				filename = objKey[lenFilesetPatch:]
				if not self.ignoreFilename(filename):
					size = item.get('Size')
					lastModified = str(item.get('LastModified'))
					lastModified = lastModified.split("+")[0]
					resultList.append(InputFile(filename, size, lastModified))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		if len(resultList) == 0:
			Log.getLogger(self.filesetId).message(Log.EROR, "%s: Invalid bucket %s or prefix %s" % (invokeBy, location, gfFilesetPath))

		return resultList

	def gfThumbnailFiles(self):
		# s3 listobjects from s3://dbp-vid/video/thumbnails
		gfFilesetPath = "video/thumbnails/"
		gfThumbnailFilesArray = self.getListFilesFromBucket(self.location, gfFilesetPath, len(gfFilesetPath), "Get GF ThumbnailFiles")

		s3Thumbnails = []

		for thumbnailFile in gfThumbnailFilesArray:
			if (thumbnailFile.startswith("gf")):
				ext = thumbnailFile.name.split(".")[-1]

				if ext in { "jpg", "tif", "png" }:
					s3Thumbnails.append(thumbnailFile.name)

		return s3Thumbnails

	## This method is used to download files to local disk when needed for processing
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
		# this assumes Sofria-client has been called
		if len(self.files[0].name) < 9:
			if self.locationType == InputFileset.LOCAL:
				directory = self.fullPath() + os.sep
			else:
				directory = self.config.directory_upload_aws + self.filesetPath + os.sep # download path
			bibleDB = SqliteUtility(processedFileset.databasePath)
			# tableContents entity must have been populated by sofria-client. And the code column means book code value
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
		if self.typeCode == "text":
			if len(self.filesetId) < 10:
				print ("DEBUG: text filesetid less than 10 characters long: " + self.filesetId)

		return self.filesetId


if (__name__ == '__main__'):
	from InputProcessor import *	
	from LanguageReaderCreator import LanguageReaderCreator

	config = Config()
	s3Client = AWSSession.shared().s3Client
	session = boto3.Session(profile_name = config.s3_aws_profile)
	s3Client = session.client('s3')
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	InputFileset.validate = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
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
		print(inp.toString())
		print("subtype", inp.subTypeCode())
	Log.writeLog(config)

# python3 load/InputFileset.py test s3://etl-development-input Spanish_N2SPNTLA_USX # works after refactor
# python3 load/InputFileset.py test s3://etl-development-input ENGESVN2DA # works after refactor
# python3 load/InputFileset.py test s3://etl-development-input SLUYPMP2DV # works after refactor


# python3 load/InputFileset.py test /Volumes/FCBH/files/complete/audio/ENGESV/ ENGESVN2DA ENGESVN2DA16

# python3 load/InputFileset.py newdata s3://dbp-prod ENGESVN2DA ENGESVN2DA16

# time python3 load/InputFileset.py test s3://test-dbp-etl HYWWAVN2ET


# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Abidji N2ABIWBT/05 DBP & GBA/Abidji_N2ABIWBT/Abidji_N2ABIWBT_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Acholi N2ACHBSU/05 DBP & GBA/Acholi_N2ACHBSU - Update/Acholi_N2ACHBSU_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Achuar [ACU]/N2ACUTBL/05 DBP & GBA/Achuar_N2ACUTBL - Update/Achuar_N2ACUTBL_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Agni Sanvi N2ANYWBT/05 DBP & GBA/Agni Sanvi_N2ANYWBT/Agni Sanvi_N2ANYWBT_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Agta, Pahanan N2APFCMU/05 DBP & GBA/Agta, Pahanan_N2APFCMU/Agta, Pahanan_N2APFCMU_USX"
# python3 load/InputFileset.py test s3://dbp-etl-mass-batch "Akan [AKA]/N1AKABIB (Asante)/05 DBP & GBA/Akan, Asante_N1AKABIB/Akan, Asante_N1AKABIB_USX"







