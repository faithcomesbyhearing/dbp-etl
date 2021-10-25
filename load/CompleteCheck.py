# CompleteCheck.py

# This program performs completeness checks of the DBP database.

# Before running this program, one must a current copy of dbp-prod.txt, and dbp-vid.txt
# These are generated by DownloadBucketList.py

import io
from datetime import datetime
from Config import *
from SQLUtility import *
from LPTSExtractReader import *
from DownloadBucketList import *
from DBPRunFilesS3 import *


class CompleteCheck:

	TEXT_FILE = "complete-check.txt"
	HTML_FILE = "complete-check.html"

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.textOut = open(CompleteCheck.TEXT_FILE, "w")
		self.htmlOut = open(CompleteCheck.HTML_FILE, "w")
		self.htmlOut.write("<html><body><h1>Complete Check Report</h1>\n<table><caption>")
		now = datetime.now()
		self.htmlOut.write(now.strftime("%A, %d %b %Y %-I:%-M %p"))
		self.htmlOut.write("\n</caption>\n")
		self.missingS3Objects = []


	def close(self):
		self.htmlOut.write("</table><h2>Missing Objects</h2><table>\n")
		for obj in sorted(self.missingS3Objects):
			self.textOut.write("%s NOT in s3 bucket.\n" % (obj,))
			self.htmlOut.write("<tr><td>%s NOT in s3 bucket.</td></tr>\n" % (obj,))
		self.textOut.close()
		self.htmlOut.write("</table></body></html>")
		self.htmlOut.close()


	# Check for each file in bible_files to insure that it exists in the DBP
	def bibleFilesToS3(self, dbpProdModified):
		files = set()
		filesets = set()
		missingFilesetIds = set()
		absentFilesetIds = set()
		dbProd = io.open(self.config.directory_bucket_list + "dbp-prod.txt", mode="r", encoding="utf-8")
		for line in dbProd:
			if "delete" not in line:
				fullFilename = line.split("\t")[0]
				files.add(fullFilename)
				parts = fullFilename.split("/")
				if len(parts) > 3:
					fileset = "%s/%s/%s/" % (parts[0], parts[1], parts[2])
					filesets.add(fileset)
		dbProd.close()
		print(len(files), "dbp-prod records")
		dbVid = io.open(self.config.directory_bucket_list + "dbp-vid.txt", mode="r", encoding="utf-8")
		for line in dbVid:
			fullFilename = line.split("\t")[0]
			if fullFilename.endswith(".m3u8"):
				files.add(fullFilename)
				parts = fullFilename.split("/")
				if len(parts) > 3:
					fileset = "%s/%s/%s/" % (parts[0], parts[1], parts[2])
					filesets.add(fileset)
		dbVid.close()
		print(len(files), "dbp-prod + dbp-vid records")
		sql = ("SELECT f.set_type_code, c.bible_id, f.id, bf.file_name"
			" FROM bible_filesets f, bible_files bf, bible_fileset_connections c"
			" WHERE f.hash_id = bf.hash_id AND f.hash_id = c.hash_id"
			" AND bf.updated_at < %s"
			" ORDER BY f.id, c.bible_id")
		resultSet = self.db.select(sql, (dbpProdModified))
		for (typeCode, bibleId, filesetId, filename) in resultSet:
			if len(filesetId) < 10 or filesetId[-2:] != "SA":
				fullKey = "%s/%s/%s/%s" % (typeCode.split("_")[0], bibleId, filesetId, filename)
				if fullKey not in files:
					self.missingS3Objects.append(fullKey)
					missingFilesetIds.add(filesetId)
				prefixKey = "%s/%s/%s/" % (typeCode.split("_")[0], bibleId, filesetId)
				if prefixKey not in filesets:
					absentFilesetIds.add(filesetId)
		for filesetId in sorted(missingFilesetIds):
			if filesetId not in absentFilesetIds:
				self.printErr("%s Has some files missing from s3 bucket" % (filesetId))
		for filesetId in sorted(absentFilesetIds):
			self.printErr("%s Has all files missing from s3 bucket." % (filesetId))


	# Check each DamId in LPTS to see if it has been loaded into DBP
	def bibleFilesetsToLPTS(self):
		missing = []
		filesetIds = self.db.selectSet("SELECT id FROM bible_filesets", ())
		for rec in self.lptsReader.resultSet:
			for typeCode in ["audio", "text", "video"]:
				apiPermiss = None
				appPermiss = None
				webPermiss = None
				indexes = [1, 2, 3]
				if typeCode == "audio":
					apiPermiss = rec.APIDevAudio()
					appPermiss = rec.DBPMobile()
					webPermiss = rec.DBPWebHub()
				if typeCode == "text":
					apiPermiss = rec.APIDevText()
					appPermiss = rec.MobileText()
					webPermiss = rec.HubText()
				if typeCode == "video":
					apiPermiss = rec.APIDevVideo()
					appPermiss = rec.MobileVideo()
					webPermiss = rec.WebHubVideo()
					indexes = [1]
				if apiPermiss == "-1" or appPermiss == "-1" or webPermiss == "-1":
					for index in indexes:
						damIdMap = rec.DamIdMap(typeCode, index)
						for (damId, status) in damIdMap.items():
							if damId[-3:] != "_ET":
								if status in {"Live", "live"}:
									if damId not in filesetIds:
										missing.append("%s %s %d %s Has no bible_filesets record." % (damId, typeCode, index, rec.Reg_StockNumber()))
		for missed in sorted(missing):
			self.printErr(missed)


	def checkForMissingTS(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
		 						' (SELECT hash_id FROM bible_files WHERE id IN'
		 						' (SELECT bible_file_id FROM bible_file_stream_bandwidths WHERE id NOT IN'
		 						' (SELECT stream_bandwidth_id FROM bible_file_stream_ts)))', ())
		for missed in sorted(missing):
			self.printErr("%s Has some or all bible_file_stream_ts records missing" % (missed,))


	def checkForMissingBandwidth(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
								' (SELECT hash_id FROM bible_files WHERE id NOT IN'
								' (SELECT bible_file_id FROM bible_file_stream_bandwidths))', ())
		for missed in sorted(missing):
			self.printErr("%s Has some or all bible_file_stream_bandwidths records missing" % (missed,))


	def checkForMissingBibleFiles(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code NOT IN'
								' ("text_plain") AND hash_id NOT IN (SELECT hash_id FROM bible_files)', ())
		for missed in sorted(missing):
			self.printErr("%s Has some or all bible_files records missing" % (missed,))


	def checkForMissingBibleVerses(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code IN ("text_plain")'
							' AND hash_id NOT IN (SELECT hash_id FROM bible_verses)', ())
		for missed in sorted(missing):
			self.printErr("%s Has some or all bible_verses records missing" % (missed,))


	def bibleIdCheck(self):
		sql = "SELECT f.id, c.bible_id FROM bible_filesets f, bible_fileset_connections c WHERE f.hash_id=c.hash_id"
		filesetMap = db.selectMap(sql, ())
		for record in self.lptsReader.resultSet:
			stockNo = record.Reg_StockNumber()
			text = record.DamIdList("text")
			audio = record.DamIdList("audio")
			video = record.DamIdList("video")
			damIds = text + audio + video 
			for (damId, index, status, fieldname) in damIds:
				if status in { "Live", "live" }:
					lptsBibleId = record.DBP_EquivalentByIndex(index)
					dbpBibleId = filesetMap.get(damId)
					if dbpBibleId == None:
						dbpBibleId = filesetMap.get(damId[:6])
					if dbpBibleId == None:
						self.printErr("%s %s(%s) %s has no filesetId in DBP" % (stockNo, damId, fieldname, status))
					elif dbpBibleId != lptsBibleId:
						self.printErr("%s %s(%s) %s has bibleId %s, but does not match %s in DBP" % (stockNo, damId, fieldname, status, lptsBibleId, dbpBibleId))


	def printErr(self, line):
		print(line)
		self.textOut.write(line + "\n")
		self.htmlOut.write("<tr><td>%s</td></tr>\n" % (line,))


if (__name__ == '__main__'):
	config = Config()
	if len(sys.argv) > 2 and sys.argv[2].lower() == "full":
		bucket = DownloadBucketList(config)
		pathname = bucket.listBucket("dbp-prod")
		DBPRunFilesS3.simpleUpload(config, pathname, "text/plain")
		dbpProdModified = datetime.utcnow()
		pathname = bucket.listBucket("dbp-vid")
		DBPRunFilesS3.simpleUpload(config, pathname, "text/plain")
		dbpVidModified = dbpProdModified
	elif len(sys.argv) > 2 and sys.argv[2].lower() == "fast":
		pathname = config.directory_bucket_list + "dbp-prod.txt"
		DBPRunFilesS3.simpleDownload(config, pathname)
		dbpProdModified = DBPRunFilesS3.simpleLastModified(config, "dbp-prod.txt")
		pathname = config.directory_bucket_list + "dbp-vid.txt"
		DBPRunFilesS3.simpleDownload(config, pathname)
		dbpVidModified = DBPRunFilesS3.simpleLastModified(config, "dbp-vid.txt")
	elif len(sys.argv) > 2 and sys.argv[2].lower() == "restart":
		dbpProdModified = DBPRunFilesS3.simpleLastModified(config, "dbp-prod.txt")
		dbpVidModified = DBPRunFilesS3.simpleLastModified(config, "dbp-vid.txt")
		print(dbpProdModified, dbpVidModified)
	else:
		print("Usage: python3 load/CompleteCheck.py  config_profile  full|fast|restart")
		sys.exit()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)
	check = CompleteCheck(config, db, lptsReader)
	check.bibleFilesToS3(dbpProdModified)
	check.bibleFilesetsToLPTS()
	check.checkForMissingTS()
	check.checkForMissingBandwidth()
	check.checkForMissingBibleFiles()
	check.checkForMissingBibleVerses()
	check.bibleIdCheck()
	check.close()
	db.close()
	DBPRunFilesS3.simpleUpload(config, CompleteCheck.TEXT_FILE, "text/plain")
	DBPRunFilesS3.simpleUpload(config, CompleteCheck.HTML_FILE, "text/html")


# python3 load/CompleteCheck.py test retry
# python3 load/CompleteCheck.py newdata > complete.out


"""
if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)
	check = CompleteCheck(config, db, lptsReader)
	check.bibleIdCheck()
"""

