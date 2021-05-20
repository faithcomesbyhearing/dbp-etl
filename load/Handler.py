# Handler.py

# AWS Lambda Handler for PreValidate


import boto3
from LPTSExtractReader import *
from PreValidate import *
​
def handler(event, context):
	session = boto3.Session()
	s3 = session.client("s3")
	print("Copying lpts-dbp.xml...")
	s3.download_file(os.getenv("UPLOAD_BUCKET"), "lpts-dbp.xml", "/tmp/lpts-dbp.xml")
	lptsReader = LPTSExtractReader("/tmp/lpts-dbp.xml")
	validate = PreValidate(lptsReader)
	filesetId = event["prefix"]
	prefix = validate.validateFilesetId(filesetId)
	if prefix != None and len(validate.messages) == 0:
		(lptsRecord, damId, typeCode, bibleId, index) = prefix
		if typeCode == "text":
			filesetId = filesetId[:6]
		validate.validateLPTS(typeCode, filesetId, lptsRecord, index)
	validate.printLog()
	return validate.messages