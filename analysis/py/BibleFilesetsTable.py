# BibleFilesetsTable.py
#
#
#| id            | varchar(16) | NO   | MUL | NULL              |                             |
#| hash_id       | char(12)    | NO   | PRI | NULL              |                             |
#| asset_id      | varchar(64) | NO   | MUL |                   |                             |
#| set_type_code | varchar(16) | NO   | MUL | NULL              |                             |
#| set_size_code | char(9)     | NO   | MUL | NULL              |                             |
#| hidden        | tinyint(1)  | NO   |     | 0                 |                             |

import io
import os
import sys
import hashlib
from Config import *
from BucketReader import *
from VersesReader import *

class BibleFilesetsTable:

	def __init__(self, config):
		self.config = config
		bucket = BucketReader(config)
		self.filesets = bucket.filesets()
		print("num filesets %d" % (len(self.filesets)))
		verseReader = VersesReader(config)
		self.verseFilesets = verseReader.filesetIds()
		print("num verse filesets %d" % (len(self.verseFilesets)))


	def assetId(self, fileset):
		return fileset[1]


	def setTypeCode(self, fileset):
		typeCode = fileset[2]
		if typeCode == "app":
			return "app"
		elif typeCode == "audio":
			damId = fileset[0]
			code = damId[7:10]
			if code == "1DA":
				return "audio"
			elif code == "2DA":
				return "audio_drama"
			else:
				print("WARNING: file type not known for %s fileset is dropped" % (damId))
				return None
		elif typeCode == "text":
			return "text_format"
		elif typeCode == "video":
			return "video_stream"
		elif typeCode == "verse":
			return "text_plain"
		else:
			print("ERROR typeCode '%s' is not known" % (typeCode))
			sys.exit()		



	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def setSizeCode(self):
		# This must be set by query of bible_files
		# S is used here, because a valid size code is required.
		return "S"


	def hidden(self):
		return 0


config = Config()
bible = BibleFilesetsTable(config) 
results = []

for fileset in bible.filesets:
	filesetId = fileset[0]
	#print(filesetId)
	assetId = bible.assetId(fileset)
	setTypeCode = bible.setTypeCode(fileset)
	if setTypeCode != None:
		hashId = bible.hashId(assetId, filesetId, setTypeCode)
		setSizeCode = bible.setSizeCode()
		hidden = bible.hidden()
		results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden))

for filesetId in bible.verseFilesets:
	#print(filesetId)
	assetId = config.s3_bucket
	setTypeCode = "text_plain"
	hashId = bible.hashId(assetId, filesetId, setTypeCode)
	setSizeCode = bible.setSizeCode()
	hidden = bible.hidden()
	results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden))

print("num records to insert %d" % (len(results)))
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_filesets (id, hash_id, asset_id, set_type_code, set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, %s)", results)
outputDB.close()



