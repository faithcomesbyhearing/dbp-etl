# S3Utility
#
# This file performs the S3Upload operations
#
#

import boto3
import os
import shutil
import subprocess
from Log import *
from Config import *
from AWSSession import *
from InputFileset import *
from TranscodeVideo import *
from FilenameParser import *
from AWSTranscoder import *

class S3Utility:

	def __init__(self, config):
		self.config = config
		AWSSession.shared() # ensure init


	def getAsciiObject(self, s3Bucket, s3Key):
		obj = AWSSession.shared().s3Client.get_object(Bucket=s3Bucket, Key=s3Key)
		content = obj['Body'].read().decode('ascii')
		return content


	def uploadAllFilesets(self, filesets):
		for inp in filesets:
			s3Bucket = self.config.s3_vid_bucket if inp.typeCode == "video" else self.config.s3_bucket
			if inp.typeCode == "video":
				self.uploadFileset(s3Bucket, inp)
			elif inp.typeCode == "audio":
				self.uploadFileset(s3Bucket, inp)
				transcoder = AWSTranscoder(self.config)
				outFilesets = transcoder.transcodeAudio(inp)
				parser = FilenameParser(self.config)
				parser.process3(outFilesets) # create accepted.csv
				for outFileset in outFilesets:
					if os.path.isfile(outFileset.csvFilename): # test if it was accepted
						InputFileset.database.append(outFileset)			 		
			elif inp.typeCode == "text":
				subTypeCode = inp.subTypeCode()
				if inp.subTypeCode() == "text_plain":
					InputFileset.database.append(inp) ## Temp until text_format is ready
				else:
					done = self.uploadFileset(s3Bucket, inp)


	def uploadFileset(self, s3Bucket, inputFileset):
		inp = inputFileset
		profile = AWSSession.shared().role_profile()
		if inp.locationType == InputFileset.LOCAL:
			source = inp.fullPath()
		else:
			source = "s3://%s" % (inp.fullPath())
		target = "s3://%s/%s" % (s3Bucket, inp.filesetPrefix)
		cmd = "aws %s s3 sync --acl bucket-owner-full-control \"%s\" \"%s\"" % (profile, source, target)
		print("upload:", cmd)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0:
			Log.getLogger(inp.filesetId).message(Log.FATAL, "ERROR: Upload of %s to %s failed. MESSAGE: %s" % (inp.filesetPrefix, s3Bucket, response.stderr))
			return False
		else:
			if inp.filesetPrefix.startswith("video"):
				TranscodeVideo.transcodeVideoFileset(self.config, inp.filesetPrefix, inp.s3FileKeys())
			InputFileset.database.append(inp)
			return True


if (__name__ == '__main__'):
	from SQLUtility import *
	from InputProcessor import *
	from UpdateDBPTextFilesets import *
	from LanguageReaderCreator import LanguageReaderCreator	

	config = Config.shared()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	filesets = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)

	s3 = S3Utility(config)

	db = SQLUtility(config)
	texts = UpdateDBPTextFilesets(config, db, None)
	results = []
	for inp in filesets:
		if inp.typeCode == "text":
			if inp.locationType == InputFileset.BUCKET:
				filePath = inp.downloadFiles()
			else:
				filePath = inp.fullPath()
			errorTuple = texts.validateFileset("text_plain", inp.bibleId, inp.filesetId, inp.languageRecord, inp.index, filePath)
			if errorTuple != None:
				logger = Log.getLogger(inp.filesetId)
				logger.messageTuple(errorTuple)
			else:
				htmlFormatFileset = texts.createTextFileset(inp)
				results.append(htmlFormatFileset)

	s3.uploadAllFilesets(filesets + results)

# python3 load/S3Utility.py test $ETL_LOCAL/ ENGESVN2DA

# Successful tests with source on local drive
# time python3 load/S3Utility.py test /Volumes/FCBH/all-dbp-etl-test/ ENGESVN2DA ENGESVN2DA16
# time python3 load/S3Utility.py test /Volumes/FCBH/all-dbp-etl-test/ HYWWAVN2ET
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV

# Successful tests with source on s3
# time python3 load/S3Utility.py test s3://etl-development-input ENGESVN2DA
# time python3 load/S3Utility.py test s3://test-dbp-etl ENGESVN2DA
# time python3 load/S3Utility.py test s3://test-dbp-etl text/ENGESV/ENGESVN2DA16
# time python3 load/S3Utility.py test s3://test-dbp-etl HYWWAVN2ET
# time python3 load/S3Utility.py test s3://test-dbp-etl ENGESVP2DV

# Some video uploads
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ ENGESVP2DV
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ video/ENGESV/ENGESVP2DV
# time python3 load/S3Utility.py test-video /Volumes/FCBH/all-dbp-etl-test/ video/ENGESX/ENGESVP2DV



