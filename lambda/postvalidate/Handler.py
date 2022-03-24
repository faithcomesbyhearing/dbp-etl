from ValidatePage import *

def handler(event, context):
	page = ValidatePage()
	hashIdList = page.getHashIds(None, event["filesetId"])
	page.process(None, hashIdList)
	page.close()
	with open('/tmp/sample.html') as f:
	    return f.read()
