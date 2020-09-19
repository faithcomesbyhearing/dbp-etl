Configuration File

Name: dbp-etl.cfg
Location: users home directory

# This configuration file must be placed in the user's home directory.
# It allows different configurations that are separated by [name]
# [dev], [test], [stage], [prod] are suggested names, but any name can be added
# The name [DEFAULT] can proceed settings that are default, if not overridden.
# The configuration name is the first command-line parameter to any dbp load program

[dev]

database.tunnel = ???? (optional)
database.host	= 127.0.0.1
database.user	= ????
database.passwd = ????
database.port	= ????
database.db_name = dbp
database.user_db_name = dbp_users

s3.bucket		= dbp-prod
s3.vid_bucket	= dbp-vid
s3.aws_profile	= FCBH_DBP_DEV

## directories for staging files being processed
directory.validate	= /????/files/validate/
directory.uploading = /????/files/uploading/
directory.uploaded	= /????/files/uploaded/
directory.database	= /????/files/database/
directory.complete	= /????/files/complete/

## directories for storing in-process files
directory.quarantine = /????/files/active/quarantine/
directory.duplicate = /????/files/active/duplicate/
directory.accepted = /????/files/active/accepted/
directory.transcoded = /????/files/active/transcoded/

## audio transcoder settings
audio.transcoder.region	= us-east-1
audio.transcoder.input_bucket = test-dbp-audio-transcode
audio.transcoder.pipeline = 1600444320877-xz1xsy
audio.preset.mp3_16bit = 1600450911341-sbjykx
audio.preset.mp3_32bit = 1600450983559-8mnddf
audio.preset.mp3_64bit = 1600451005783-fvtcgc

directory.errors = /????/files/validate/errors/
error.limit.pct = 25.0

directory.bucket_list  = /????/files/active/
filename.lpts_xml	   = /????/files/active/qry_dbp4_Regular_and_NonDrama.xml
filename.accept.errors = /????/files/validate/AcceptErrors.txt

filename.datetime	= %y-%m-%d-%H-%M-%S

[dev_audio_hls]

database.host 	= localhost
database.user 	= root
database.passwd = ????
database.port	= 3306
database.db_name = hls_dbp

s3.bucket = dbp-prod
s3.vid_bucket = dbp-vid
s3.aws_profile = FCBH_Gary
directory.audio_hls = /Users/garygriswold/FCBH/files/tmp
audio.hls.duration.limit = 10

[test]

[stage]

[prod]
