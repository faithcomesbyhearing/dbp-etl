# Handler.py

# AWS Lambda Handler for DatabaseCheck


import os
import boto3
from Config import *
from SQLUtility import *
from DatabaseCheck import *

def handler(event, context):
	#migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE") # Should be "B" or "C"

#	directory = event["prefix"] # can be filesetId or lang_stockno_USX

	config = Config()
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	db = SQLUtility(config)	
	check = DatabaseCheck(config, db, languageReader)
	check.process()
	check.close()
	db.close()
