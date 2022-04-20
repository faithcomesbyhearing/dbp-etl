# InputProcessor.py

# This class processes the directory loaded by the etl-web user. Based on the contents of the directory and whether the metadata exists in LPTS, 
# one or more InputFileset objects are created and returned.
# 
import os
from datetime import datetime
from Log import *
from Config import *
from InputFileset import *
from RunStatus import *
from PreValidate import *
from AWSSession import *
from UnicodeScript import *

class InputProcessor:

	## parse command line, and return [InputFileset]
	def commandLineProcessor(config, s3Client, lptsReader):
		if len(sys.argv) < 4:
			print("FATAL command line parameters: config_profile  s3://bucket|localPath  path_list ")
			sys.exit()

		results = []
		location = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
		paths = sys.argv[3:]

		# LoadController is called with this: 2022-04-15-16-24-20/French_N1 & O1 FRABIB_USX
		# location should be 2022-04-15-16-24-20/French_N1 & O1 FRABIB_USX

		# the CLI allows a list of paths. The primary use case is to provide one path only
		for path in paths:
			# the CLI allows a path to contain multiple directories. The primary use case is for the path to contain a single directory
			path = path[:-1] if path.endswith("/") else path
			directory = path.split("/")[-1]

			# this call will return a list of PreValidateResult objects containing information on validated filesets
			preValidate = PreValidate(lptsReader, s3Client, location)
			(dataList, messages) = preValidate.validateDBPETL(s3Client, location, directory, path)
			for data in dataList:
				inp = InputFileset(config, location, data.filesetId, path, data.damId, 
					data.typeCode, data.bibleId(), data.index, data.lptsRecord, data.fileList)
				#print("INPUT", inp.toString())
				results.append(inp)
			if messages != None and len(messages) > 0:
				print("Validate.filesetCommandLineParser...setting RunStatus to fail for fileset %s because validate returned messages %s" % (directory, messages))
				RunStatus.set(directory, False)
				Log.addPreValidationErrors(messages)
				sys.exit()
				
			if (dataList == None or len(dataList) == 0) and (messages == None or len(messages) == 0):
				Log.getLogger(path).message(Log.FATAL, "Unknown Error in InputProcessor.commandLineParser")
				Log.writeLog(config)
				sys.exit()

		return results

	def toString(self):
		results = []
		results.append("InputFileset\n")
		results.append("location=" + self.location + "\n")
		results.append("locationType=" + self.locationType)
		results.append("prefix=%s/%s/%s" % (self.typeCode, self.bibleId, self.filesetId))
		results.append(" damId=" + self.lptsDamId)
		results.append(" stockNum=" + self.stockNum())
		results.append(" index=" + str(self.index))
		results.append(" script=" + str(self.lptsRecord.Orthography(self.index)) + "\n") #?? probably overcopied
		results.append("filesetPrefix=" + self.filesetPrefix + "\n")
		results.append("csvFilename=" + self.csvFilename + "\n") #?? probably overcopied
		for file in self.files:
			results.append(file.toString() + "\n")
		return " ".join(results)

	def stockNum(self):
		return self.lptsRecord.Reg_StockNumber()

	def fullPath(self):
		if self.locationType == InputFileset.LOCAL:
			return self.location + os.sep + self.filesetPath
		else:
			return self.location + "/" + self.filesetPath



if (__name__ == '__main__'):
	config = Config()
	session = boto3.Session(profile_name = config.s3_aws_profile)
	s3Client = session.client('s3')
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	InputFileset.validate = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, lptsReader)
	for inp in InputFileset.validate:
		print("INPUT", inp.toString())		
	Log.writeLog(config)

#python3 load/InputProcessor.py test s3://etl-development-input Spanish_N2SPNTLA_USX # works after refactor
#python3 load/InputProcessor.py test s3://etl-development-input "French_N2FRN PDC PDF PDV_USX"
#python3 load/InputProcessor.py test s3://etl-development-input "French_N1 & O1 FRABIB_USX" # works after refactor, when stocknumber.txt added
#python3 load/InputProcessor.py test s3://etl-development-input ENGESVN2DA # works after refactor

#python3 load/InputProcessor.py test s3://etl-development-input SLUYPMP2DV # works after refactor