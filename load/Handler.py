# Handler.py

# AWS Lambda Handler for PreValidate


import boto3
from LPTSExtractReader import *
from PreValidate import *


def handler(event, context):
	directory = event["prefix"] # can be filesetId or lang_stockno_USX
	filenames = event["files"] # Should be object keys
	bucket    = os.getenv("UPLOAD_BUCKET")

	session = boto3.Session()
	s3Client = session.client("s3")
	print("Copying lpts-dbp.xml...")
	s3Client.download_file(bucket, "lpts-dbp.xml", "/tmp/lpts-dbp.xml")
	languageReader = LanguageReaderCreator().createWithPath("/tmp/lpts-dbp.xml")
	
	preValidate = PreValidate(languageReader, UnicodeScript(), s3Client, bucket)
	messages = preValidate.validateLambda(languageReader, directory, filenames)
	return messages
