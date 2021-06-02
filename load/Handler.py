# Handler.py

# AWS Lambda Handler for PreValidate


import boto3
from LPTSExtractReader import *
from PreValidate import *


def handler(event, context):
	session = boto3.Session()
	s3 = session.client("s3")
	print("Copying lpts-dbp.xml...")
	s3.download_file(os.getenv("UPLOAD_BUCKET"), "lpts-dbp.xml", "/tmp/lpts-dbp.xml")
	lptsReader = LPTSExtractReader("/tmp/lpts-dbp.xml")
	validate = PreValidate(lptsReader)
	directory = event["prefix"] # can be filesetId or lang_stockno_USX
	filenames = event["files"] # Should be object keys
	messages = PreValidate(lptsReader, directory, filenames)
	return messages