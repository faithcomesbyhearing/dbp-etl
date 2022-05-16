import tempfile
from ValidatePage import *

def handler(event, context):
	page = ValidatePage()
	filesetId =  event["filesetId"]
	page.process(filesetId)
	page.close()
	with open(tempfile.gettempdir() + "/sample.html") as f:
	    return f.read()
