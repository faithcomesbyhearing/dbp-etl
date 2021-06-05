# UpdateDBPBibleFilesSecondary

import os
import sys
import zipfile
import boto3
import subprocess
from Log import *
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from LPTSExtractReader import *
from AWSSession import *


class UpdateDBPBibleFilesSecondary:

	## How do failures in processing get passed to log, and effect the RunStatus


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db 
		self.dbOut = dbOut


	def createAllZipFiles(self, inputFilesetList):
		for inp in inputFilesetList:
			if inp.typeCode == "audio" and inp.isMP3Fileset():
				if inp.zipFile() == None:
					target = self.config.s3_bucket + "/" + self.filesetPath
					zipDirectory = self.getZipInternalDir(inp.damId, inp.lptsRecord)
					data = { "source": inp.fullPath(), "target": target, "directory": zipDirectory }
					response = self.httpPost(self.config.audio_zip_url, self.config.audio_zip_key, data)
					if response.get("status") != "SUCCESS":
						error = response.get("error", "")
						error = "Error in zip " + error
						Log.getLogger(inp.filesetId).message(Log.EROR, error)


	def getZipInternalDir(self, damId, lptsRecord):
		langName = lptsRecord.LangName()
		iso3 = lptsRecord.ISO()
		stockNo = lptsRecord.Reg_StockNumber()
		versionCode = stockNo[-3] if stockNo != None else ""
		scope = damId[6]
		if scope == "N" or scope == "O":
			scope += "T"
		style = "Drama" if damId[7] == "2" else "Non-Drama"
		return "%s_%s_%s_%s_%s" % (langName, iso3, versionCode, scope, style)


	def httpPost(self, url, key, data):
		content = data.encode('utf-8')
		req = urllib.request.Request(url, data=content)
		req.add_header("Content-Type", "application/json")
		req.add_header("X-API-Key", key)
		resp = urllib.request.urlopen(req)
		out = resp.read()
		result = out.decode('utf-8')
		return json.loads(result)


	def updateBibleFilesSecondary(self, hashId, inputFileset):
		inp = inputFileset
		insertRows = []

		sql = "SELECT file_name FROM bible_files_secondary WHERE hash_id = %s AND file_type = %s"
		if inp.typeCode == "audio":

			dbpArtSet = self.db.selectSet(sql, (hashId, 'art'))
			for artFile in inp.artFiles():
				if not artFile in dbpArtSet:
					insertRows.append(('art', hashId, artFile))

			if inp.isMP3Fileset() and len(inp.lptsDamId) == 10:
				zipFile = inp.zipFile()
				if zipFile != None:
					dbpZipSet = self.db.selectSet(sql, (hashId, 'zip'))
					if not zipFile.name in dbpZipSet:
						insertRows.append(('zip', hashId, zipFile.name))

		tableName = "bible_files_secondary"
		pkeyNames = ("hash_id", "file_name")
		attrNames = ("file_type",)
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


#	def zipAudio(self, bucket, filesetPrefix):
#		zipObjName = None
#		isOK = False
#		directory = self.downloadFileset(bucket, filesetPrefix)
#		if directory != None:
#			zipFile = self.zipAudioFileset(directory)
#			zipObjName = filesetPrefix + os.sep + os.path.basename(filesetPrefix) + ".zip" #### this needs correcting
#			isOK = self.uploadZipFile(bucket, zipObjName, zipFile)
#		return zipObjName if isOK else None


#	def downloadFileset(self, bucket, filesetPrefix):
#		profile = AWSSession.shared().profile()
#		if not bucket.startswith("s3://"):
#			bucket = "s3://" + bucket
#		source = bucket + "/" + filesetPrefix
#		target = "/tmp/" + os.path.basename(filesetPrefix)
#		cmd = "aws %s s3 sync --acl bucket-owner-full-control %s %s" % (profile, source, target)
#		print("upload:", cmd)
#		response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
#		if response.returncode != 0:
#			print("ERROR: Download of %s to %s failed. MESSAGE: %s" % (source, target, response.stderr))
#			return None
#		else:
#			return target
#
#
#	def zipAudioFileset(self, directory):
#		zipfilePath = "/tmp/result.zip"
#		if os.path.isfile(zipfilePath):
#			os.remove(zipfilePath)
#		zipDir = zipfile.ZipFile(zipfilePath, "w")
#		with zipDir:
#			#filesetId = os.path.basename(directory)
#			localDirectory = self.getZipInternalDir()
#			for file in os.listdir(directory):
#				if file.endswith(".mp3") and not file.startswith("."):
#					fullPath = directory + os.sep + file
#					localPath = localDirectory + os.sep + file
#					zipDir.write(fullPath, localPath)
#		return zipfilePath
#
#
#	def uploadZipFile(self, bucket, zipObjName, zipFile):
#		try:
#			response = AWSSession.shared().s3Client.upload_file(zipFile, bucket, zipObjName)
#		except ClientError as e:
#			print("ERROR: Upload of %s to %s/%s failed." % (zipFile, bucket, zipObjName))
#			return False
#		return True


	## debug routine
	def downloadZipFile(self, bucket, zipObjName, zipFile):
		try:
			response = AWSSession.shared().s3Client.download_file(bucket, zipObjName, zipFile)
		except ClientError as e:
			print("ERROR: Download of %s/%s to %s failed." % (bucket, zipObjName, zipFile))


if (__name__ == '__main__'):
	from PreValidate import *
	from InputFileset import *

	config = Config.shared()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPBibleFilesSecondary(config, db, dbOut)
	s3Client = AWSSession.shared().s3Client
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)

	sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
			" WHERE f.hash_id = c.hash_id AND set_type_code in ('audio', 'audio_drama')")
	resultSet = db.select(sql, ())
	for (bibleId, filesetId, hashId) in resultSet:
		#if bibleId in { "BCCWBT", "BCCWBTR", "BGTNEW", "CAKSOU", "DGDANT", "UNRWFW" }:
		if filesetId in { "UNRWFWP1DA" }:
			print("1.", bibleId, filesetId, hashId)
			location = "s3://dbp-etl-upload-dev-zrg0q2rhv7shv7hr"
			#filesetPath = "audio/%s/%s" % (bibleId, filesetId)
			filesetPath = filesetId
			(data, messages) = PreValidate.validateDBPELT(lptsReader, s3Client, location, filesetId, filesetPath)
			if data != None:
				print("2.", data.toString(), messages)
				inputFileset = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
								data.typeCode, data.bibleId, data.index, data.lptsRecord)
				if len(inputFileset.files) > 0:
					print("3.", inputFileset.toString())
					update.updateBibleFilesSecondary(hashId, inputFileset)
					dbOut.displayStatements()
					dbOut.displayCounts()
					#success = dbOut.execute("test-" + filesetId)
					#print(success)
			elif messages != None and len(messages) > 0:
				Log.addPreValidationErrors(messages)
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
