# Validate.py

import os
import sys
import zipfile
from datetime import datetime
from Config import *
from Log import *
from SQLUtility import *
from FilenameParser import *
from FilenameReducer import *
from UpdateDBPTextFilesets import *


class Validate:	

	def __init__(self, config, db):
		self.config = config
		self.db = db
		self.scriptNameSet = db.selectSet("SELECT lpts_name FROM lpts_script_codes", ())
		self.numeralsSet = db.selectSet("SELECT id FROM numeral_systems", ())


	def process(self, filesets):
		self.prepareDirectory(self.config.directory_accepted)
		self.prepareDirectory(self.config.directory_quarantine)
		self.prepareDirectory(self.config.directory_duplicate)
		self.prepareDirectory(self.config.directory_errors)

		## Validate filetypes
		for inp in filesets:
			logger = Log.getLogger(inp.filesetId)
			for file in inp.files:
				ext = os.path.splitext(file.name)[-1]
				if inp.typeCode == "audio" and ext not in {".mp3", ".opus", ".webm", ".m4a", ".jpg", ".tif", ".png", ".zip"}:
					logger.invalidFileExt(file.name)
				elif inp.typeCode == "text" and not ext in {".html", ".usx", ".xml"}:
					logger.invalidFileExt(file.name)
				elif inp.typeCode == "video" and ext != ".mp4":
					logger.invalidFileExt(file.name)

			if inp.typeCode == "text":
				unicodeScript = UnicodeScript()
				errors = unicodeScript.validateStockNoRecord(inp.languageRecord, self.db)
				for error in errors:
					logger.message(Log.EROR, error)

		## Validate Text Filesets
		texts = UpdateDBPTextFilesets(self.config, self.db, None)
		results = []
		for inp in filesets:
			if inp.typeCode == "text":
				if inp.locationType == InputFileset.BUCKET:
					filePath = inp.downloadFiles()
				else:
					filePath = inp.fullPath()

				textplainFileset = self.validateTextPlainFilesets(texts, inp, filePath)

				if textplainFileset != None:
					results.append(textplainFileset)
					jsonFileset = self.validateTextJsonFilesets(texts, inp, filePath)

					if jsonFileset != None:
						results.append(jsonFileset)

		filesets += results

		self.validateLPTS(filesets)

		FilenameReducer.openAcceptErrorSet(self.config)
		parser = FilenameParser(self.config)
		parser.process3(filesets)

		## This was intended for processing an entire bucket.
		#find = FindDuplicateFilesets(self.config)
		#duplicates = find.findDuplicates()
		#find.moveDuplicates(duplicates)

	def validateTextPlainFilesets(self, texts, inp, filePath):
		print("validateTextPlainFilesets. 1")
		errorTuple = texts.validateFileset("text_plain", inp.bibleId, inp.filesetId, inp.languageRecord, inp.index, filePath)
		print("validateTextPlainFilesets. 1a")

		if errorTuple == None:
			print("validateTextPlainFilesets. 2")		
			errorTuple = texts.invokeBiblePublisher(inp, filePath)
			print("validateTextPlainFilesets. 2a")		

			if errorTuple == None:
				print("validateTextPlainFilesets. 3")		
				return texts.createTextFileset(inp)

		print("validateTextPlainFilesets. 4")		
		logger = Log.getLogger(inp.filesetId)
		logger.messageTuple(errorTuple)

		return None

	def validateTextJsonFilesets(self, texts, inp, filePath):
		print("validateTextJsonFilesets. 1")
		errorTuple = texts.validateFileset("text_json", inp.bibleId, inp.filesetId, inp.languageRecord, inp.index, filePath)
		print("validateTextJsonFilesets. 1a")

		if errorTuple == None:
			print("validateTextJsonFilesets. 2")
			errorTuple = texts.invokeSofriaCli(filePath, inp.textFilesetId())
			print("validateTextJsonFilesets. 2a")

			if errorTuple == None:
				print("validateTextJsonFilesets. 3")
				return texts.createJSONFileset(inp)

		print("validateTextJsonFilesets. 4")
		logger = Log.getLogger(inp.filesetId)
		logger.messageTuple(errorTuple)

		return None

	## prepareDirectory 1. Makes sure a directory exists. 2. If it contains .csv files,
	## they are packaged up into a zip file using the timestamp of the first csv file.
	def prepareDirectory(self, directory):
		if not os.path.isdir(directory):
			os.makedirs(directory)
		else:
			modifiedTime = self.getModifiedTime(directory)
			if modifiedTime != None:
				pattern = self.config.filename_datetime 
				zipfilePath = directory + modifiedTime.strftime(pattern) + ".zip"

				zipDir = zipfile.ZipFile(zipfilePath, "w")
				with zipDir:
					for file in os.listdir(directory):
						if (file.endswith(".csv") or file.endswith(".db")) and not file.startswith("._"):
							fullPath = directory + os.sep + file
							zipDir.write(fullPath, file)
							os.remove(fullPath)
 

	def getModifiedTime(self, directory):
		listDir = os.listdir(directory)
		if len(listDir) > 0:
			for file in listDir:
				if file.endswith("csv"):
					filePath = directory + os.sep + file
					filetime = os.path.getmtime(filePath)
					return datetime.fromtimestamp(filetime)
		return None


	## These validations require MySql and so are not done in PreValidate
	def validateLPTS(self, filesets):
		for inp in filesets:
			logger = Log.getLogger(inp.filesetId)
			if inp.typeCode == "text":

				scriptName = inp.languageRecord.Orthography(inp.index)
				if scriptName != None and scriptName not in self.scriptNameSet:
					logger.invalidValues(inp.stockNum(), "_x003n_Orthography", scriptName)

				numeralsName = inp.languageRecord.Numerals()
				if numeralsName != None and numeralsName not in self.numeralsSet:
					logger.invalidValues(inp.stockNum(), "Numerals", numeralsName)


if (__name__ == '__main__'):
	from DBPLoadController import *
	from LanguageReaderCreator import LanguageReaderCreator		
	from LanguageReader import *
	config = Config.shared()
	db = SQLUtility(config)
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	filesets = InputFileset.filesetCommandLineParser(config, languageReader)
	ctrl = DBPLoadController(config, db, languageReader)
	ctrl.validate(filesets)

# Successful tests with source on s3
# time python3 load/Validate.py test s3://test-dbp-etl ENGESVN2DA
# time python3 load/Validate.py test s3://test-dbp-etl text/ENGESV/ENGESVN2DA16
# time python3 load/Validate.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/Validate.py test s3://test-dbp-etl ENGESVP2DV


