# TestBiblePublisher.py
#
# This program tests the publish and validate programs one at a time from data that is stored in S3.
#
# ROOT_DIR/
#	{filesetId}/
#		source/
#			*.usx
#			metadata.xml
#		{filesetId}.db
#		output/
#			xml
#			usx
#			html
#			chapters.txt
#			verses.txt
#
"""
Is there a way to scan the top level directory of an S3 bucket?


"""
import sys
import boto3
from Config import *

class TestBiblePublisher:

	def __init__(self, config, testDirectory, stockNum):
		self.config = config
		self.testDirectory = testDirectory
		self.stockNum = stockNum
		print(testDirectory, stockNum)
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')

	def listStockNums(self, bucket):
		stockNums = []
		request = { 'Bucket': bucket, 'Delimiter': '_', 'MaxKeys':1000 }
		hasMore = True
		while hasMore:
			response = self.client.list_objects_v2(**request)
			for prefix in response['CommonPrefixes']:
				stockNums.append(prefix.get('Prefix'))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		return stockNums

	def listStockNums2(self, bucket, prefix):
		stockNums = []
		request = { 'Bucket': bucket, 'Delimiter': '_', 'MaxKeys':1000, 'Prefix': prefix }
		hasMore = True
		while hasMore:
			response = self.client.list_objects_v2(**request)
			for prefix in response['CommonPrefixes']:
				stockNums.append(prefix.get('Prefix'))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		return stockNums

	def listObjects(self, bucket, prefix):
		objects = []
		request = { 'Bucket': bucket, 'MaxKeys':1000, 'Prefix': prefix }
		hasMore = True
		while hasMore:
			response = self.client.list_objects_v2(**request)
			for item in response['Contents']:
				objects.append(item.get('Key'))
			hasMore = response['IsTruncated']
			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']
		return objects				

"""
		this.fs = require('fs');
		this.child = require('child_process');
		this.directory = directory;
		this.versionId = versionId;
		this.versions = [];
		this.programs = ['BuildPublisher', 'XMLTokenizerTest', 'USXParserTest', 'HTMLValidator', 
					'VersesValidator', 'StyleUseValidator', 'TableContentsValidator' ];//, 'ConcordanceValidator'];
		this.programs = ['BuildPublisher'];
		//session = boto3.Session(profile_name=config.s3_aws_profile)
		//self.client = session.client('s3')
	}
	execute() {
		//try:
		//	print("Download %s, %s to %s" % (s3Bucket, s3Key, filename))
		//	self.client.download_file(s3Bucket, s3Key, filename)
		//	print("Done With Download")
		//except Exception as err:
		//	print("ERROR: Download %s failed with error %s" % (s3Key, err))

		// unzip version
		this.executeNext(-1);
	}
	downloadFile() {
		session = boto3.Session(profile_name=config.s3_aws_profile)
		self.client = session.client('s3')
	}
	executeNext(programIndex) {
		if (++programIndex < this.programs.length) {
			this.executeOne(programIndex);
		}
	}
	executeOne(programIndex) {
		var that = this;
		var program = this.programs[programIndex];
		const command = this.commandLine(program, this.versionId);
		console.log(command);
		var options = {maxBuffer:1024*1024*8}; // process killed with no error code if buffer size exceeded
		this.child.exec(command, options, function(error, stdout, stderr) {
			if (error) {
				that.errorMessage(command, error);
			}
			console.log(output);
			that.executeNext(programIndex);
		});
	}
	commandLine(program, version) {
		const rootDir = this.directory + "/" + version;
		if (program === "unzip") {
			return `${program} ${version}.zip -d ${this.directory}/`
		} else if (program === "BuildPublisher") {
			return `../publish/${program}.sh ${rootDir}/ ${rootDir}/ ${version} eng null ltr`;
		} else if (program === "XMLTokenizerTest") {
			return `${program}.sh ${rootDir}/source ${rootDir}/ ${rootDir}/output ${version}`;
		} else if (program === "USXParserTest") {
			return `${program}.sh ${rootDir}/source ${rootDir}/ ${rootDir}/output ${version}`;
		} else if (program === "HTMLValidator") {
			return `${program}.sh ${rootDir}/source ${rootDir}/ ${rootDir}/output ${version}`;
		} else if (program === "StyleUseValidator") {
			return `${program}.sh ${rootDir}/ ${rootDir}/output ${version}`;
		} else if (program === "VersesValidator") {
			return `${program}.sh ${rootDir}/ ${rootDir}/output ${version}`;
		} else if (program === "TableContentsValidator") {
			return `${program}.sh ${rootDir}/ ${version}`;
		} else if (program === "ConcordanceValidator") {
			return `${program}.sh ${rootDir}/ ${rootDir}/output ${version}`;
		} else {
			return `${program}.sh ${rootDir}/source ${rootDir}/ ${rootDir}/output ${version}`;
		}
	}
	errorMessage(description, version, program, error) {
		console.log('ERROR:', description, JSON.stringify(error));
		console.log(version, program);
		process.exit(1);
	}
}
"""

if len(sys.argv) < 4:
	print("Usage: python3 TestBiblePublisher.js  config_profile root_directory  stockNum")
	sys.exit()

rootDirectory = sys.argv[2]
stockNum = sys.argv[3]

config = Config()
test = TestBiblePublisher(config, rootDirectory, stockNum);
stockNums = test.listStockNums("chnunvn2da.shortsands.com");
for stockNum in stockNums:
	print(stockNum)
stockNums2 = test.listStockNums2("chnunvn2da.shortsands.com", stockNums[0])
for stockNum in stockNums2:
	print(stockNum)
objects = test.listObjects("chnunvn2da.shortsands.com", stockNums2[0])
for obj in objects:
	print(obj)

