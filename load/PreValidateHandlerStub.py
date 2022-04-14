# PreValidateHandlerStub.py
# This is a test stub intended to provide the same interface as the AWS Lambda Handler
import boto3
from LPTSExtractReader import *
from PreValidate import *
from UnicodeScript import *
from Config import *

def handler(event, context):
    config = Config.shared()
    session = boto3.Session(profile_name = config.s3_aws_profile)
    s3Client = session.client("s3")
    bucket = event["bucket"]
    prefix = event["prefix"]
    stocknumbers = event["stocknumbers"]
    unicodeScript = UnicodeScript()
    print("Copying lpts-dbp.xml...")
    languageReader = LanguageReaderCreator().create(config)
    preValidate = PreValidate(languageReader, unicodeScript, s3Client, bucket)

    testDataMap = {}
    request = { 'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1000 }
    hasMore = True

    while hasMore:
        response = s3Client.list_objects_v2(**request)
        hasMore = response['IsTruncated']
        for item in response.get('Contents', []):
            objKey = item.get('Key')
            (directory, filename) = objKey.rsplit("/", 1)
            files = testDataMap.get(directory, [])
            files.append(filename)
            testDataMap[directory] = files
        if hasMore:
            request['ContinuationToken'] = response['NextContinuationToken']

    messages = []
    for (directory, filenames) in testDataMap.items():
        messages.append(preValidate.validateLambda(directory, filenames, stocknumbers))
    
    print("messages: ", messages)

    return messages

if (__name__ == '__main__'):
    prefix = sys.argv[2][:-1] if sys.argv[2].endswith("/") else sys.argv[2]
    stocknumbersContents = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != None else None
    bucket = "etl-development-input"
    handler(
        {
            "prefix": prefix,
            "bucket": bucket,
            "stocknumbers": stocknumbersContents
        }, None)
