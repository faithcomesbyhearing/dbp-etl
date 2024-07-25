# Handler.py

# AWS Lambda Handler for PreValidate


import os
import boto3
from LanguageReaderCreator import LanguageReaderCreator
from LanguageReader import *
from TextStockNumberProcessor import *
from PreValidate import *

def handler(event, context):
	bucket    = os.getenv("UPLOAD_BUCKET")
	migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE", "B") # Should be "B" or "BLIMP"
	print("prevalidate. migration_stage: %s" % migration_stage)
	directory = event["prefix"] # can be filesetId or lang_stockno_USX
	filenames = event["files"] # Should be object keys
	# Should be a string if user has uploaded a file.
	# e.g. "N1KANDPI\r\nO1KANDPI"
	# each stocknumber will be separated by a line break.
	stocknumbersContents = event["stocknumbers"]

	session = boto3.Session()
	s3Client = session.client("s3")
	xmlFile = ""
	if migration_stage != "BLIMP":
		print("Copying lpts-dbp.xml...")
		xmlFile = "/tmp/lpts-dbp.xml"
		s3Client.download_file(bucket, "lpts-dbp.xml", xmlFile)
	print("prevalidate. 5")

	languageReader = LanguageReaderCreator(migration_stage).create(xmlFile)
	print("prevalidate. 10")
	
	preValidate = PreValidate(languageReader, s3Client, bucket)
	print("prevalidate. 20")
	messages = preValidate.validateLambda(directory, filenames, stocknumbersContents)
	print("prevalidate. 30")
	return messages
