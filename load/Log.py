# This class collects and reports error messages

from Config import *
from DBPRunFilesS3 import *

class Log:

	FATAL = 0
	EROR = 1
	WARN = 2
	INFO = 3
	loggers = {}

	def getLogger(filesetId):
		logger = Log.loggers.get(filesetId)
		if logger == None:
			logger = Log(filesetId)
			Log.loggers[filesetId] = logger
		return logger

	def fatalError(message):
		logger = Log.getLogger("~")
		logger.message(Log.FATAL, message)
		config = Config()
		Log.writeLog(config)
		sys.exit()

	def totalErrorCount():
		count = 0
		for logger in Log.loggers.values():
			count += logger.errorCount()
		return count

	def writeLog(config):
		errors = []
		for key in sorted(Log.loggers.keys()):
			logger = Log.loggers[key]
			for message in logger.format():
				errors.append(message)

		if len(errors) > 0:
			errorDir = config.directory_errors
			pattern = config.filename_datetime 
			path = errorDir + "Errors.out"
			print("openErrorReport", path)
			errorFile = open(path, "a", encoding="utf-8")
			for message in errors:
				errorFile.write(message + '\n')
				print(message, end='\n')
			errorFile.close()
			DBPRunFilesS3.uploadFile(config, path)
		print("Num Errors ", len(errors))


	def addPreValidationErrors(messages):
		for (filesetId, errors) in messages.items():
			logger = Log.getLogger(filesetId)
			for error in errors:
				logger.messages.append((Log.EROR, error))	


	def __init__(self, filesetId):
		self.filesetId = filesetId
		self.messages = []

#	def hasMessages(self):
#		return len(self.messages) > 0

	def errorCount(self):
		count = 0;
		for msg in self.messages:
			if msg[0] == Log.EROR or msg[0] == Log.FATAL:
				count += 1
		return count

	def message(self, level, text):
		self.messages.append((level, text))

	def messageTuple(self, messageTuple):
		self.messages.append(messageTuple)

	def invalidFileExt(self, filename):
		self.messages.append((Log.EROR, "/%s has an invalid file ext." % (filename)))

	def missingBibleIds(self):
		self.messages.append((Log.EROR, "bibleId is not in Blimp. Contact the Agreements team."))

	def missingBibleIdConnection(self, filesetId):
		self.messages.append((Log.EROR, "FilesetId: %s is not related with a specific bible." % (filesetId)))

	def missingFilesetIds(self):
		self.messages.append((Log.EROR, "filesetId is not in Blimp. Contact the Agreements team."))

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
				self.messages.append((Log.EROR, "%s/%s %s." % (self.filesetId, file.file, ", ".join(file.errors))))

	def format(self):
		levelMap = { Log.FATAL: "FATAL", Log.EROR: "EROR", Log.WARN: "WARN", Log.INFO: "INFO"}
		output = []
		for (level, msg) in self.messages:
			levelMsg = levelMap.get(level)
			output.append("%s %s %s" % (levelMsg, self.filesetId, msg))
		return output


if (__name__ == '__main__'):
	config = Config()
	error = Log.getLogger("ENGESVN2DA")
	error.message(Log.INFO, "First message")
	error.messageTuple((Log.WARN, "Second message"))
	error.invalidFileExt("MyFilename")
	error.missingBibleIds()
	error.missingFilesetIds()
	error.requiredFields("aStockNo", "aFieldName")
	error.suggestedFields("aStockNo", "aFieldName")
	error.invalidValues("afieldName", "aFieldValue")
	Log.writeLog(config)

