# TestBiblePublisher.py
#
# This program tests the publish and validate programs one at a time from data that is stored in S3.
#
# ROOT_DIR/
#	{filesetId}/
#		*.usx
#		metadata.xml
#		{filesetId}.db
#		output/
#			xml
#			usx
#			html
#			chapters.txt
#			verses.txt
#

import sys
import subprocess
#import boto3
from Config import *
from LPTSExtractReader import *

class TestBiblePublisher:

	def __init__(self, config, testDirectory, bucket, stockNum):
		self.config = config
		self.testDirectory = testDirectory
		self.bucket = bucket
		self.lptsStockNum = stockNum
		self.textStockNum = stockNum.replace("/", "")
		print(testDirectory, stockNum)
		self.publisherRoot = os.path.dirname(os.path.dirname(config.publisher_js))
		print("publisherRoot", self.publisherRoot)
		#session = boto3.Session(profile_name=config.s3_aws_profile)
		#self.client = session.client('s3')
		#self.client = config.s3_client
		self.programs = ["publish", "xml", "usx", "html", "verses", "toc"] # "style", "concordance"

	def findStockNumEntry(self):
		filePath = self.config.directory_bucket_list + self.bucket
		fp = open(filePath, "r")
		for line in fp:
			if self.textStockNum in line:
				fp.close()
				return line.strip()
		fp.close()
		print("Stock Number %s is not found" % (self.textStockNum))
		return None

	def syncTextFileset(self, objectKey):
		cmd = "aws --profile %s s3 sync s3://%s/%s %s/%s " % (self.config.s3_aws_profile, s3Bucket, objectKey, self.testDirectory, self.textStockNum)
		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
		if response.returncode != 0
			print("ERROR: Download of %s to %s/%s failed. MESSAGE: %s" % (objectKey, self.testDirectory, self.textStockNum, response.stderr))
			return False
		else:
			return True


	#def listStockNums(self):
	#	stockNums = []
	#	request = { 'Bucket': self.bucket, 'Delimiter': '_', 'MaxKeys':1000 }
	#	hasMore = True
	#	while hasMore:
	#		response = self.client.list_objects_v2(**request)
	#		for prefix in response['CommonPrefixes']:
	#			stockNums.append(prefix.get('Prefix'))
	#		hasMore = response['IsTruncated']
	#		if hasMore:
	#			request['ContinuationToken'] = response['NextContinuationToken']
	#	return stockNums
	#
	#def listStockNums2(self, prefix):
	#	stockNums = []
	#	request = { 'Bucket': self.bucket, 'Delimiter': '_', 'MaxKeys':1000, 'Prefix': prefix }
	#	hasMore = True
	#	while hasMore:
	#		response = self.client.list_objects_v2(**request)
	#		for prefix in response['CommonPrefixes']:
	#			stockNums.append(prefix.get('Prefix'))
	#		hasMore = response['IsTruncated']
	#		if hasMore:
	#			request['ContinuationToken'] = response['NextContinuationToken']
	#	return stockNums
	#
	#def listObjects(self, prefix):
	#	objects = []
	#	request = { 'Bucket': self.bucket, 'MaxKeys':1000, 'Prefix': prefix }
	#	hasMore = True
	#	while hasMore:
	#		response = self.client.list_objects_v2(**request)
	#		for item in response['Contents']:
	#			objects.append(item.get('Key'))
	#		hasMore = response['IsTruncated']
	#		if hasMore:
	#			request['ContinuationToken'] = response['NextContinuationToken']
	#	return objects
	#
	#def downloadObjects(self, keys):
	#	directory = self.testDirectory + "/" + self.stockNum
	#	os.mkdir(directory)
	#	for key in keys:
	#		try:
	#			filename = directory + "/" + key
	#			print("Download %s, %s to %s" % (self.bucket, key, filename))
	#			self.client.download_file(self.bucket, key, filename)
	#			print("Done With Download")
	#		except Exception as err:
	#			print("ERROR: Download %s failed with error %s" % (key, err))

	def execute(self, program):
		cmd = self.command(program)
		print("cmd:", cmd)
		response = subprocess.run(cmd, shell=False, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=2400)
		success = response.returncode == 0
		print("stderr:", str(response.stderr.decode('utf-8')))
		print(str(response.stdout.decode('utf-8')))

	def command(self, program):
		if program == "publish":
			exePath = "%s/publish" % (self.publisherRoot)
		else:
			exePath = "%s/validate" % (self.publisherRoot)
		print("exePath", exePath)
		os.chdir(exePath)
		source = self.testDirectory + "/text/" + self.stockNum + "/" + self.stockNum
		if program == "publish":
			return ["BuildPublisher.sh", source, source, self.stockNum, "eng", "en", "ltr"]
		elif program == "xml":
			return ["XMLTokenizerTest.sh", source, source, source + "/output", self.stockNum]
		elif program == "usx":
			return ["USXParserTest.sh", source, source, source + "/output", self.stockNum]
		elif program == "html":
			return ["HTMLValidator.sh", source, source, source + "/output", self.stockNum]
		elif program == "style":
			return ["StyleUseValidator.sh", source, source + "/output", self.stockNum]
		elif program == "verses":
			return ["VersesValidator.sh", source, source + "/output", self.stockNum]
		elif program == "toc":
			return ["TableContentsValidator.sh", source, self.stockNum]
		elif program == "concordance":
			return ["ConcordanceValidator.sh", source, source + "/output", self.stockNum]
		else:
			print("ERROR: Unknown program %s" % (program))


if len(sys.argv) < 4:
	print("Usage: python3 TestBiblePublisher.js  config_profile root_directory  stockNum  [program]")
	sys.exit()

rootDirectory = sys.argv[2]
stockNum = sys.argv[3]
if len(sys.argv) > 4:
	program = sys.argv[4]
else:
	program = None

config = Config()
test = TestBiblePublisher(config, rootDirectory, "dbp-etl-mass-batch", stockNum)
objectKey = test.findStockNumEntry()
if objectKey != None:
	if test.syncTextFileset(objectKey):
		if program != None:
			test.execute(program)
		else:
			for program in test.programs:
				test.execute(program)

