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
from LPTSExtractReader import *
from SQLUtility import *
from LookupTables import *
from BucketReader import *

class BibleFilesetsTable:

	def __init__(self, config):
		self.config = config
		bucket = BucketReader(config)
		bucket.filesetIds()
		self.audioIds = bucket.audioIdList
		self.textIds = bucket.textIdList
		self.videoIds = bucket.videoIdList
		## I might not need the DB ??
		self.inputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_input_db_name)
		## I might not need the Extract ??
		reader = LPTSExtractReader(config)
		self.audioMap = reader.getAudioMap()
		print("num audio filesets in list", len(self.audioMap.keys()))
		self.textMap = reader.getTextMap()
		#print(self.textMap.keys())
		print("num text filesets in list", len(self.textMap.keys()))
		## We also need to read the video bucket here !!!!


	def assetIdAudioText(self, damId):
		return config.s3_bucket


	def assetIdVideo(self, damId):
		return config.s3_vid_bucket


	def appTypeCode(self, damId):
		return "app"


	def setAudioTypeCode(self, damId):
		code = damId[7:10]
		if code == "1DA":
			return "audio"
		elif code == "2DA":
			return "audio_drama"
		else:
			print("WARNING: file type not known for %s" % damId)
			return "unknown"


	def setTextTypeCode(self, damId):
		return "text_format"


	def setVideoTypeCode(self, damId):
		return "video_stream"


	def hashId(self, damId, bucket, typeCode):
		md5 = hashlib.md5()
		md5.update(damId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def setSizeCode(self, damId):
		# This must be set by query of bible_files
		# S is used here, because a valid size code is required.
		return "S"


	def hidden(self, damId):
		return 0


config = Config()
filesets = BibleFilesetsTable(config) 
results = []

"""
for appId in filesets.appIds:
	print(appId)
	app = filesets.audioMap.get(appId)
	if app != None:
		print("app", appId)
		assetId = fileset.assetIdAudioText(appId)
		setTypeCode = fileset.setAudioTypeCode(appId)
		hashId = fileset.hashId(appId, assetId, setTypeCode)
		setSizeCode = fileset.setSizeCode(appId)
		hidden = fileset.hidden(appId)
		results.append((appId, hashId, assetId, setTypeCode, setSizeCode, hidden))
	else:
		print("WARNING LPTS has no record for app %s" % (appId))
"""
for audioId in filesets.audioIds:
	#print(audioId)
	audio = filesets.audioMap.get(audioId[:10])
	if audio != None:
		#print("audio", audioId)
		assetId = filesets.assetIdAudioText(audioId)
		setTypeCode = filesets.setAudioTypeCode(audioId)
		hashId = filesets.hashId(audioId, assetId, setTypeCode)
		setSizeCode = filesets.setSizeCode(audioId)
		hidden = filesets.hidden(audioId)
		results.append((audioId, hashId, assetId, setTypeCode, setSizeCode, hidden))
	else:
		print("WARNING LPTS has no record for audio %s" % (audioId))

for textId in filesets.textIds:
	#print(textId)
	text = filesets.textMap.get(textId)
	if text != None:
		#print("text", textId)
		assetId = filesets.assetIdAudioText(textId)
		setTypeCode = filesets.setTextTypeCode(textId)
		hashId = filesets.hashId(textId, assetId, setTypeCode)
		setSizeCode = filesets.setSizeCode(textId)
		hidden = filesets.hidden(textId)
		results.append((textId, hashId, assetId, setTypeCode, setSizeCode, hidden))
	else:
		print("WARNING LPTS has no record for text %s" % (textId))

for videoId in filesets.videoIds:
	#print(videoId)
	video = filesets.videoMap.get(videoId)
	if video != None:
		#print("video", videoId)
		assetId = filesets.assetIdVideo(videoId)
		setTypeCode = filesets.setVideoTypeCode(videoId)
		hashId = filesets.hashId(videoId, assetId, setTypeCode)
		setSizeCode = filesets.setSizeCode(videoId)
		hidden = filesets.hidden(videoId)
		results.append((videoId, hashId, assetId, setTypeCode, setSizeCode, hidden))
	else:
		print("WARNING LPTS has no record for video %s" % (videoId))


filesets.inputDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bible_filesets (id, hash_id, asset_id, set_type_code, set_size_code, hidden) VALUES (%s, %s, %s, %s, %s, %s)", results)
outputDB.close()


