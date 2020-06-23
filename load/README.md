


# This configuration file must be placed in the user's home directory.
# It allows different configurations that are separated by [name]
# [dev], [test], [stage], [prod] are suggested names, but any name can be added
# The configuration name is the first command-line parameter to any program
# that uses this configuration file

[dev]

database.host	= localhost
database.user	= root
database.passwd = brilligg
database.db_name= valid_dbp
database.port	= 3306

s3.bucket		= dbp-prod
s3.vid_bucket	= dbp-vid
s3.aws_profile	= FCBH_Gary

directory.bucket_list	= /Users/garygriswold/FCBH/bucket_data/

#directory.validate	= /Users/garygriswold/FCBH/files/validate/
directory.validate	= /Volumes/FCBH/FCBH_9_11_test_data/
directory.upload	= /Users/garygriswold/FCBH/files/upload/
directory.database	= /Users/garygriswold/FCBH/files/database/
directory.complete	= /Users/garygriswold/FCBH/files/complete/

## Should these be directories, or should these be part of the filename
directory.quarantine = /Users/garygriswold/FCBH/files/validate/quarantine/
directory.duplicate = /Users/garygriswold/FCBH/files/validate/duplicate/
directory.accepted = /Users/garygriswold/FCBH/files/validate/accepted/

directory.errors = /Users/garygriswold/FCBH/files/validate/errors/
error.limit.pct = 25.0

filename.lpts_xml	= /Users/garygriswold/FCBH/bucket_data/qry_dbp4_Regular_and_NonDrama.xml
filename.accept.errors = /Users/garygriswold/FCBH/files/validate/AcceptErrors.txt

filename.datetime	= %y-%m-%d-%H-%M-%S

[dev_audio_hls]

database.host 	= localhost
database.user 	= root
database.passwd = brilligg
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
