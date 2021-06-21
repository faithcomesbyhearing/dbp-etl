# UpdateDBPBibleFilesSecondary

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
from LPTSExtractReader import *
from AWSSession import *


class UpdateDBPBibleFilesSecondary:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db 
		self.dbOut = dbOut


	def createAllZipFiles(self, inputFilesetList):
		for inp in inputFilesetList:
			self.createZipFile(inp)


	def createZipFile(self, inputFileset):
		inp = inputFileset
		if inp.typeCode == "audio" and inp.isMP3Fileset() and len(inp.filesetId) == 10:
			source = "s3://%s/%s/" % (self.config.s3_bucket, inp.filesetPrefix)
			target = "s3://%s/%s/%s.zip" % (self.config.s3_bucket, inp.filesetPrefix, inp.filesetId)
			zipDirectory = self.getZipInternalDir(inp.lptsDamId, inp.lptsRecord)
			fileTypes = ["mp3"]
			data = { "source": source, "target": target, "directoryName": zipDirectory, "fileTypes": fileTypes }
			#print(data)
			dataJson = json.dumps(data)
			dataBytes = dataJson.encode("utf-8")
			lambdaClient = AWSSession.shared().lambdaInvoker()
			try:
				response = lambdaClient.invoke(
													FunctionName = self.config.lambda_zip_function,
													InvocationType = "RequestResponse",
													Payload = dataBytes)
				respData = response.get("Payload").read()
				#print("respData", respData)
				respStr = respData.decode("utf-8")
				result = json.loads(respStr)
				status = result.get("status")
				inp.reloadFilenames() # Need to get zipfile into inputFileset
				if status != "SUCCESS":
					result = json.loads(result.get("body"))
					Log.getLogger(inp.filesetId).message(Log.EROR, "Error in zip file creation %s" % (result.get("error"),))
			except Exception as err:
				Log.getLogger(inp.filesetId).message(Log.EROR, "Failure in zip file creation %s" % (err,))


	def getZipInternalDir(self, damId, lptsRecord):
		langName = lptsRecord.LangName()
		iso3 = lptsRecord.ISO()
		stockNo = lptsRecord.Reg_StockNumber()
		versionCode = stockNo[-3:] if stockNo != None else ""
		scope = damId[6]
		if scope == "N" or scope == "O":
			scope += "T"
		style = "Drama" if damId[7] == "2" else "Non-Drama"
		return "%s_%s_%s_%s_%s" % (langName, iso3, versionCode, scope, style)


	def updateBibleFilesSecondary(self, hashId, inputFileset):
		inp = inputFileset
		insertRows = []

		sql = "SELECT file_name FROM bible_files_secondary WHERE hash_id = %s AND file_type = %s"
		if inp.typeCode == "audio":

			dbpArtSet = self.db.selectSet(sql, (hashId, 'art'))
			for artFile in inp.artFiles():
				if not artFile in dbpArtSet:
					insertRows.append(('art', hashId, artFile))

			if inp.isMP3Fileset() and len(inp.filesetId) == 10:
				zipFile = inp.zipFile()
				if zipFile != None:
					dbpZipSet = self.db.selectSet(sql, (hashId, 'zip'))
					if not zipFile.name in dbpZipSet:
						insertRows.append(('zip', hashId, zipFile.name))

		tableName = "bible_files_secondary"
		pkeyNames = ("hash_id", "file_name")
		attrNames = ("file_type",)
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


if (__name__ == '__main__'):
	from PreValidate import *
	from InputFileset import *

	config = Config.shared()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPBibleFilesSecondary(config, db, dbOut)
	#s3Client = AWSSession.shared().s3Client
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)

	sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
			" WHERE f.hash_id = c.hash_id AND set_type_code in ('audio', 'audio_drama') AND length(f.id) = 10")
	resultSet = db.select(sql, ())
	for (bibleId, filesetId, hashId) in resultSet:
		s3Client = AWSSession().s3Client
		#print(bibleId, filesetId, hashId)
		location = "s3://%s" % (config.s3_bucket,)
		filesetPath = "audio/%s/%s" % (bibleId, filesetId)
		(dataList, messages) = PreValidate.validateDBPELT(lptsReader, s3Client, location, filesetId, filesetPath)
		if messages != None and len(messages) > 0:
			Log.addPreValidationErrors(messages)
			print(filesetPath, messages)
		if dataList == None or len(dataList) == 0:
			print("NO InputFileset", filesetPath)
		else:
			for data in dataList:
				print(data.toString())
				inp = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
								data.typeCode, data.bibleId, data.index, data.lptsRecord)
				print(inp.toString())
				if inp.zipFile() == None:
					print("must create zip file")
					update.createZipFile(inp)
				update.updateBibleFilesSecondary(hashId, inp)
				dbOut.displayStatements()
				dbOut.displayCounts()
				dbOut.execute("zip_art_" + filesetId)	
	Log.writeLog(config)


"""
CREATE TABLE bible_files_secondary (
  hash_id VARCHAR(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  file_name VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  file_type VARCHAR(12) NOT NULL,
  CHECK (file_type IN ('art', 'font', 'zip')),
  PRIMARY KEY (hash_id, file_name),
  CONSTRAINT FK_bible_filesets_bible_files_secondary FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`)
)

"""
