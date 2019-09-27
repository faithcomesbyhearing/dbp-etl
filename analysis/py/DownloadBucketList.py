# DownloadBucketList
#
# This program lists all of the keys in a bucket into a text file
# It stores this in the metadata directory where it can be used
#

import boto3
import io
import os
import sys

#BUCKET_NAME = "dbp-prod"
#BUCKET_NAME = "dbp-vid"
BUCKET_NAME = "dbs-web"

filename = "new_%s.txt" % (BUCKET_NAME.replace("-", "_"))
pathname = "%s/FCBH/bucket_data/%s" % (os.environ['HOME'], filename)
print(pathname)

out = io.open(pathname, mode="w", encoding="utf-8")

#session = boto3.Session(profile_name='FCBH_Gary')
session = boto3.Session(profile_name='FCBH_DBS')
client = session.client('s3')

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
		if (size > 0):
			try:
				#print(key)
				out.write("%s\n" % (key))
			except Exception as err:
				print("Could not write key %s" % (err))

	if hasMore:
		request['ContinuationToken'] = response['NextContinuationToken']

out.close()

