# Handler.py

# AWS Lambda Handler for PreValidate


import boto3
from LanguageReaderCreator import LanguageReaderCreator
from LanguageReader import *
from TextStockNumberProcessor import *
from PreValidate import *

def handler(event, context):
	directory = event["prefix"] # can be filesetId or lang_stockno_USX
	filenames = event["files"] # Should be object keys

	# Should be a string if user has uploaded a file.
	# e.g. "N1KANDPI\r\nO1KANDPI"
	# each stocknumber will be separated by a line break.
	stocknumbersContents = event["stocknumbers"]
	bucket    = os.getenv("UPLOAD_BUCKET")

	session = boto3.Session()
	s3Client = session.client("s3")
	print("Copying lpts-dbp.xml...")
	s3Client.download_file(bucket, "lpts-dbp.xml", "/tmp/lpts-dbp.xml")
	languageReader = LanguageReaderCreator().createWithPath("/tmp/lpts-dbp.xml")
	
	preValidate = PreValidate(languageReader, s3Client, bucket)
	messages = preValidate.validateLambda(directory, filenames, stocknumbersContents)
	return messages
