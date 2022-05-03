# Handler.py

# AWS Lambda Handler for DatabaseCheck


import os
import boto3
from DatabaseCheck import *

def handler(event, context):
	#migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE") # Should be "B" or "C"

#	directory = event["prefix"] # can be filesetId or lang_stockno_USX

	
	databaseCheck = DatabaseCheck()
	messages = databaseCheck.process()
	return messages
