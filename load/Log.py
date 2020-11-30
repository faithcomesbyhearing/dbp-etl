# This class collects and reports error messages

from datetime import datetime
from Config import *

class Log:

	FATAL = 0
	EROR = 1
	WARN = 2
	INFO = 3
	loggers = {}

	def getLogger(filesetPrefix):
		logger = Log.loggers.get(filesetPrefix)
		if logger == None:
			logger = Log(filesetPrefix)
			Log.loggers[filesetPrefix] = logger
		return logger

	def getLogger3(typeCode, bibleId, filesetId):
		filesetPrefix = "%s/%s/%s" % (typeCode, bibleId, filesetId)
		return Log.getLogger(filesetPrefix)

	def fatalError(message):
		logger = Log.getLogger("~")
		logger.message(Log.FATAL, message)
		config = Config()
		Log.writeLog(config)
		sys.exit()

	def writeLog(config):
		errors = []
		for key in sorted(Log.loggers.keys()):
			logger = Log.loggers[key]
			for message in logger.format():
				errors.append(message)

		if len(errors) > 0:
			errorDir = config.directory_errors
			pattern = config.filename_datetime 
			path = errorDir + "Errors-" + datetime.today().strftime(pattern) + ".out"
			print("openErrorReport", path)
			errorFile = open(path, "w")
			for message in errors:
				errorFile.write(message)
				print(message, end='\n')
			errorFile.close()
		print("Num Errors ", len(errors))		


	def __init__(self, filesetPrefix):
		self.filesetPrefix = filesetPrefix
		self.messages = []

#	def hasMessages(self):
#		return len(self.messages) > 0

	def errorCount(self):
		count = 0;
		for msg in self.messages:
			if msg[0] == Log.EROR:
				count += 1
		return count

	def message(self, level, text):
		self.messages.append((level, text))

	def messageTuple(self, messageTuple):
		self.messages.append(messageTuple)

	def invalidFileExt(self, filename):
		self.messages.append((Log.EROR, "/%s has an invalid file ext." % (filename)))

	def missingBibleIds(self):
		self.messages.append((Log.EROR, "bibleId is not in LPTS."))

	def missingFilesetIds(self):
		self.messages.append((Log.EROR, "filesetId is not in LPTS record."))

	def damIdStatus(self, stockNo, status):
		self.messages.append((Log.WARN, "LPTS %s has status = %s." % (stockNo, status)))

	def requiredFields(self, stockNo, fieldName):
		self.messages.append((Log.EROR, "LPTS %s field %s is required." % (stockNo, fieldName)))

	def suggestedFields(self, stockNo, fieldName):
		self.messages.append((Log.WARN, "LPTS %s field %s is missing." % (stockNo, fieldName)))

	def invalidValues(self, stockNo, fieldName, fieldValue):
		self.messages.append((Log.EROR, "in %s %s has invalid value '%s'." % (stockNo, fieldName, fieldValue)))

	def fileErrors(self, fileList):
		for file in fileList:
			if len(file.errors) > 0:
				self.messages.append((Log.EROR, "%s/%s %s." % (self.filesetPrefix, file.file, ", ".join(file.errors))))

	def format(self):
		levelMap = { Log.FATAL: "FATAL", Log.EROR: "EROR", Log.WARN: "WARN", Log.INFO: "INFO"}
		output = []
		for (level, msg) in self.messages:
			levelMsg = levelMap.get(level)
			output.append("%s %s %s" % (levelMsg, self.filesetPrefix, msg))
		return output


if (__name__ == '__main__'):
	config = Config()
	#error = Log.factory("text/ENGESV/ENGESVN2DA")
	error = Log.getLogger("text/ENGESV/ENGESVN2DA")
	error.message(Log.INFO, "First message")
	error.messageTuple((Log.WARN, "Second message"))
	error.invalidFileExt("MyFilename")
	error.missingBibleIds()
	error.missingFilesetIds()
	error.requiredFields("aStockNo", "aFieldName")
	error.suggestedFields("aStockNo", "aFieldName")
	error.invalidValues("afieldName", "aFieldValue")
	Log.writeLog(config)

