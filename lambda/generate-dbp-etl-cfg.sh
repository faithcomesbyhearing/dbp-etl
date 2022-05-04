#!/bin/sh
set -eu

finish() {
  echo CLEANUP
  rm -rf "/efs/${S3_KEY_PREFIX}"
  echo EXECUTION FINISHED
}
trap finish EXIT


# properties which must be set externally
# DATABASE_HOST 
# DATABASE_PORT
# DATABASE_DB_NAME
# DATABASE_USER
# DATABASE_PASSWD
# S3_ARTIFACTS_BUCKET

# properties not needed for lambda, but required by Config. This will eventually be cleaned up in Config.setConfigParametersFromProfile
export DATABASE_USER_DB_NAME=foo
export S3_BUCKET=foo
export S3_VID_BUCKET=foo
export S3_KEY_PREFIX=foo

#Note: the config profile values are dev and newdata

cat > dbp-etl.cfg <<EOF
[DEFAULT]
database.user = ${DATABASE_USER}
database.passwd = ${DATABASE_PASSWD}
database.user_db_name = ${DATABASE_USER_DB_NAME}
s3.bucket = ${S3_BUCKET}
s3.vid_bucket = ${S3_VID_BUCKET}
s3.artifacts_bucket = ${S3_ARTIFACTS_BUCKET}
filename.lpts_xml = lpts-dbp.xml

# DBPLoadController
mysql.exe = /usr/bin/mysql
node.exe = /usr/bin/node
publisher.js = /app/BiblePublisher/publish/Publisher.js

# docker filesystem
directory.upload_aws = /efs/${S3_KEY_PREFIX}/etl_uploader/upload_aws/
directory.upload = /efs/${S3_KEY_PREFIX}/etl_uploader/upload/
directory.database = /efs/${S3_KEY_PREFIX}/etl_uploader/database/
directory.complete = /efs/${S3_KEY_PREFIX}/etl_uploader/complete/
directory.quarantine = /efs/${S3_KEY_PREFIX}/etl_uploader/quarantine/
directory.duplicate = /efs/${S3_KEY_PREFIX}/etl_uploader/duplicate/
directory.accepted = /efs/${S3_KEY_PREFIX}/etl_uploader/accepted/
directory.transcoded = /efs/${S3_KEY_PREFIX}/etl_uploader/transcoded/
directory.errors = /efs/${S3_KEY_PREFIX}/etl_uploader/errors/
error.limit.pct = 1.0
directory.bucket_list = /efs/${S3_KEY_PREFIX}/etl_uploader/
filename.accept.errors = /efs/${S3_KEY_PREFIX}/etl_uploader/AcceptErrors.txt
filename.datetime = %y-%m-%d-%H-%M-%S
video.transcoder.region = us-west-2
video.transcoder.pipeline = 1537458645466-6z62tx
video.preset.hls.1080p = 1556116949562-tml3vh
video.preset.hls.720p = 1538163744878-tcmmai
video.preset.hls.480p = 1538165037865-dri6c1
video.preset.hls.360p = 1556118465775-ps3fba
video.preset.web = 1351620000001-100070
audio.transcoder.url = https://gig8vjo8p5.execute-api.us-west-2.amazonaws.com/job
audio.transcoder.key = 1b5dc5708ae8d0335afdf94e421ae5f7d772e8f13b003c9d9733bce5caf34c6a
audio.transcoder.sleep.sec = 10
audio.transcoder.input = { "bucket": "\$bucket", "key": "\$prefix" }
audio.transcoder.output.0 = { "bucket": "${S3_BUCKET}", "key": "\$prefix-opus16", "bitrate": 16, "container": "webm", "codec": "opus" }
lambda.zip.function = arn:aws:lambda:us-west-2:078432969830:function:transcoding-api-create-zip-w9gxhplj7q9mju3h
lambda.zip.region = us-west-2
lambda.zip.timeout = 900

[${PROFILE}]
database.host = ${DATABASE_HOST}
database.port = ${DATABASE_PORT}
database.db_name = ${DATABASE_DB_NAME}

EOF

#echo "contents of dbp-etl.cfg: "
#cat /root/dbp-etl.cfg

