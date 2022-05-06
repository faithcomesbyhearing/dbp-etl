# Handler.py

# AWS Lambda Handler for DatabaseCheck
# Note that this is for DATA_MODEL_MIGRATION_STAGE "B" only. So instead of using the LanguageReaderCreator, we will directly 
# create the LPTSExractReader


import os
import boto3
from Config import *
from SQLUtility import *
from DatabaseCheck import *
from LPTSExtractReader import *

def handler(event, context):

	bucket    = os.getenv("UPLOAD_BUCKET")

	print("Copying lpts-dbp.xml...")
	session = boto3.Session()
	s3Client = session.client("s3")
	s3Client.download_file(bucket, "lpts-dbp.xml", "/tmp/lpts-dbp.xml")
	languageReader = LPTSExtractReader("/tmp/lpts-dbp.xml")
	config = Config()
	db = SQLUtility(config)	
	check = DatabaseCheck(config, db, languageReader)
	check.process()
	check.close()
	db.close()
