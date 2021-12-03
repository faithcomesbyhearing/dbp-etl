# DownloadBucketList
#
# This program lists all of the keys in a bucket into a text file
# It stores this in the metadata directory where it can be used
#

import boto3
import io
import os
import sys
import datetime
from Config import *
from AWSSession import *


class DownloadBucketList:

	def __init__(self, config):
		self.config = config

	def listBucket(self, bucketName, prefix):
		pathname = self.config.directory_bucket_list + "%s.txt" % (bucketName)
		out = io.open(pathname, mode="w", encoding="utf-8")

		client = AWSSession.shared().s3Client

		request = { 'Bucket':bucketName, 'Prefix':prefix,'MaxKeys':100000}
#		request = { 'Bucket':bucketName, 'Prefix':'video','MaxKeys':100000, 'StartAfter':'M' }

		hasMore = True
		while hasMore:
			response = client.list_objects_v2(**request)
			hasMore = response['IsTruncated']
			contents = response['Contents']
			for item in contents:
				key = item['Key']
				size = item['Size']
				datetime = item['LastModified']
				try:
					#print(key, size, datetime)
					out.write("%s\t%s\t%s\n" % (key, size, datetime))
				except Exception as err:
					print("Could not write key %s" % (err))

			if hasMore:
				request['ContinuationToken'] = response['NextContinuationToken']

		out.close()
		return pathname

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print("Usage: DownloadBucketList  config_name  bucket_name prefix")
		sys.exit()
	BUCKET_NAME = sys.argv[2]
	PREFIX = sys.argv[3]
	config = Config.shared()
	download = DownloadBucketList(config)
	pathname = download.listBucket(BUCKET_NAME, PREFIX)
	print("downloaded", pathname)

