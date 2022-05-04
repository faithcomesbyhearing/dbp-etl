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

#	directory = event["prefix"] # can be filesetId or lang_stockno_USX

	config = Config()
	languageReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)	
	check = DatabaseCheck(config, db, languageReader)
	check.process()
	check.close()
	db.close()
