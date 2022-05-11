# UpdateDBPBibleFileTags

import os
import sys
import zipfile
import boto3
import json
import urllib
import subprocess
from Log import *
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from AWSSession import *


class UpdateDBPBibleFileTags:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db 
		self.dbOut = dbOut



	def process(self, hashId, inputFileset): 
		inp = inputFileset
		insertRows = []

		# sql = "SELECT file_name FROM bible_files_secondary WHERE hash_id = %s AND file_type = %s"
		sql = "select file_id, book_id, chapter_start, verse_start, bft.tag, bft.value from bible_files bf "
		" left join bible_file_tags bft on bf.id = bft.file_id "
		" where hash_id = %s"
		
		### (Victor) TODO: need to match book_id, chapter_start, verse_start between bible_file data in database and the S3 filename, which needs to be parsed to determine book/chapter/verse
		### filenames from S3 will be of the form gf_luk_12_01.jpg. you'll need to do a case-insensitive compare of the bookname.
		### in some cases, the chapter and verse won't match between the bible_file data and the filename 
		# for example, the GF thumbnails for MAT 12 are for:
		# gf_mat_12_02.jpg (MAT, chapter 12, verse 2)
		# gf_mat_12_22.jpg (MAT, chapter 12, verse 22)
		# gf_mat_12_46.jpg (MAT, chapter 12, verse 46)
		# but the Gospel film segments for fileset AFRLPFP2DV are:
		# MAT 12 1
		# MAT 12 22
		# MAT 12 38
		# MAT 12 12 is easy because it matches - use the thumbnail gf_mat_12_22.jpg
		# for the other two, there is not a direct match on chapter and verse
		# so we need to find the closest verse..
		# for MAT 12 1, we'll need to use gf_mat_12_02.jpg (one verse off, thumbnail verse is greater than bible_file verse)
		# for MAT 12 38, we use gf_mat_12_46.jpg (8 verse difference)
		dbpBibleFiles = self.db.selectSet(sql, (hashId))
		for thumbnailFile in inp.thumbnailFiles():
			if not thumbnailFilename in dbpBibleFiles: # hack
				insertRows.append(('thumbnail', file_id, thumbnailFilename)) # hack, not tested

		tableName = "bible_file_tags"
		pkeyNames = ("file_id", "tag")
		attrNames = ("value")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from PreValidate import *
	from InputFileset import *

	if len(sys.argv) < 4:
		print("Usage: UpdateDBPBibleFileTags.py  your_profile  starting_bible_id   ending_bible_id")
		sys.exit()
	startingBibleId = sys.argv[2]
	endingBibleId = sys.argv[3]

	config = Config.shared()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPBibleFileTags(config, db, dbOut)
	s3Client = AWSSession.shared().s3Client
	# this is probably only for Stage B - ok to hardcode the stage 
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)

	# list all objects from thumbnails S3 prefix, convert to python collection
	# s3Thumbnails = s3 listobjects from s3://dbp-vid/video/thumbnails

	# sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
	# 		" WHERE f.hash_id = c.hash_id AND set_type_code in ('video_stream')"
	# 		" AND c.bible_id >= %s AND c.bible_id <= %s")
	sql = ("SELECT bf2.id, bf.id AS fileset_id, bf.asset_id, bf.hash_id, "
   			"bf2.book_id, bf2.chapter_start, bf2.chapter_end, bf2.verse_start "
			"FROM  bible_filesets bf  join bible_fileset_connections bfc on bfc.hash_id = bf.hash_id "
			"join bible_files bf2 on bf.hash_id = bf2.hash_id "
			"where bf.set_type_code = 'video_stream' and bfc.bible_id > %s and bfc.bible_id < %s")

	resultSet = db.select(sql, (startingBibleId, endingBibleId))
	for (bibleId, filesetId, hashId) in resultSet: 
		print(bibleId, filesetId, hashId)
		location = "s3://%s" % (config.s3_bucket,)
		# filesetPath = "audio/%s/%s" % (bibleId, filesetId)
		filesetPath = "video/%s/%s" % (bibleId, filesetId)
		(dataList, messages) = PreValidate.validateDBPETL(s3Client, location, filesetId, filesetPath)
		if messages != None and len(messages) > 0:
			Log.addPreValidationErrors(messages)
			#print(filesetPath, messages)
		if dataList == None or len(dataList) == 0:
			print("NO InputFileset", filesetPath)
		else:
			for data in dataList:
				#print(data.toString()) # usually just one entry for video (I think)
				inp = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
								data.typeCode, data.bibleId(), data.index, data.languageRecord)
				update.process(hashId, inp) 
				dbOut.displayStatements()
				dbOut.displayCounts()
				# dbOut.execute("gf_thumbnail_" + filesetId)
	Log.writeLog(config)

# python3 load/UpdateDBPBibleFileTags.py test starting_bible_id  ending_bible_id


