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


	def readAll(self):
		bucket = BucketReader(self.config)
		self.filesets = bucket.filesets()
		print("num filesets %d" % (len(self.filesets)))
		verseReader = VersesReader(self.config)
		self.verseFilesets = verseReader.filesetIds()
		print("num verse filesets %d" % (len(self.verseFilesets)))


	def setTypeCode(self, filesetId, typeCode):
		if typeCode == "app":
			return "app"
		elif typeCode == "audio":
			code = filesetId[7:9]
			if code == "1D":
				return "audio"
			elif code == "2D":
				return "audio_drama"
			else:
				code = filesetId[8:10]
				if code == "1D":
					return "audio"
				elif code == "2D":
					return "audio_drama"
				elif filesetId == "N1TUVDPI":
					return "audio"
				elif filesetId == "O1TUVDPI":
					return "audio"
				else:
					print("WARNING: file type not known for %s, set_type_code set to 'unknown'" % (filesetId))
					return "unknown"
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
bible.readAll()
results = []

for fileset in bible.filesets:
	filesetId = fileset[0]
	bucket = fileset[1]
	typeCode = fileset[2]

	assetId = bucket
	setTypeCode = bible.setTypeCode(filesetId, typeCode)
	if setTypeCode != None:
		hashId = bible.hashId(assetId, filesetId, setTypeCode)
		setSizeCode = bible.setSizeCode()
		hidden = bible.hidden()
		results.append((filesetId, hashId, assetId, setTypeCode, setSizeCode, hidden))

for filesetId in bible.verseFilesets:
	#print(filesetId)
	assetId = "dbp-verses" # Should this be in config.xml somewhere?
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



