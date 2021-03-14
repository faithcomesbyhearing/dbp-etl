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

if len(sys.argv) < 3:
	print("Usage: DownloadBucketList  config_name  bucket_name")
	sys.exit()

config = Config()
BUCKET_NAME = sys.argv[2]

pathname = config.directory_bucket_list + "new-%s.txt" % (BUCKET_NAME)
out = io.open(pathname, mode="w", encoding="utf-8")

session = boto3.Session(profile_name=config.s3_aws_profile)
client = session.client('s3')

s3 = session.resource('s3')
for bucket in s3.buckets.all():
	print("available bucket", bucket.name)

request = { 'Bucket':BUCKET_NAME, 'MaxKeys':1000 }
# Bucket, Delimiter, EncodingType, Market, MaxKeys, Prefix

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
