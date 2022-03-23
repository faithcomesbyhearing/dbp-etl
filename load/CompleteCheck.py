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

	HTML_FILE = "complete-check.html"

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.htmlOut = open(CompleteCheck.HTML_FILE, "w")
		self.htmlOut.write("<html><head>")
		styles = [ "<style>",
			"<!--",
			"caption {",
			"font-size: 16pt;",
			"padding-top: 1em;",
			"padding-bottom: 0.5em;",
			"}",
			"table {",
			"border: 1px solid black;",
			"border-collapse: collapse;",
			"}",
			"th {",
			"border: 1px solid black;",
			"background-color: #00CCFF;",
			"padding-left: 1em;",
			"padding-right: 1em;",
			"padding-top: 0.5em;",
			"padding-bottom: 0.5em;",
			"}",
			"td {",
			"border: 1px solid black;",
			"padding-top: 0.5em;",
			"padding-bottom: 0.5em;",
			"text-align: center",
			"}",
			"-->",
			"</style>" ]
		self.htmlOut.write("\n".join(styles))
		self.htmlOut.write("</head><body><h1>Complete Check Report</h1>\n")
		now = datetime.now()
		self.htmlOut.write("<h3>" + now.strftime("%A, %d %b %Y %-I:%-M %p") + "</h3>\n")
		self.missingS3Objects = []


	def close(self):
		self.outputTable("Missing Objects, not in s3 bucket:", ["s3Key"], sorted(self.missingS3Objects))
		self.htmlOut.write("</table></body></html>")
		self.htmlOut.close()


	def biblesWithoutConnects(self):
		resultSet = self.db.select("SELECT id, language_id FROM bibles WHERE id NOT IN" +
			" (SELECT bible_id FROM bible_fileset_connections)", ())
		self.outputTable("Bibles without Fileset Connections.", ["bibleid", "language_id"], resultSet)


	def filesetsWithoutConnects(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hash_id NOT IN" +
			" (SELECT hash_id FROM bible_fileset_connections) ORDER by id", ())
		self.outputTable("Filesets without Bible Connections.", ["filesetId", "hashId"], resultSet)


	def filesetsWithoutFiles(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hash_id NOT IN" +
			" (SELECT hash_id FROM bible_files) AND set_type_code != 'text_plain' ORDER by id", ())
		self.outputTable("Filesets without Files (ignoring plain text).", ["filesetId", "hashId"], resultSet)


	def filesetsWithoutVerses(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hash_id NOT IN" +
			" (SELECT hash_id FROM bible_verses) AND set_type_code = 'text_plain' ORDER by id", ())
		self.outputTable("Filesets without Verses (plain text only).", ["filesetId", "hashId"], resultSet)


	def biblesWithoutTitles(self):
		resultSet = self.db.select("SELECT distinct id FROM bibles WHERE id IN" +
			" (SELECT bible_id FROM bible_fileset_connections)" +
			" AND id NOT IN (SELECT bible_id FROM bible_translations) ORDER by id", ())
		self.outputTable("Bibles without Titles (and have filesets).", ["filesetId", "hashId"], resultSet)


	def biblesWithoutBooks(self):
		resultSet = self.db.select("SELECT distinct id FROM bibles WHERE id IN" +
			" (SELECT bible_id FROM bible_fileset_connections)" +
			" AND id NOT IN (SELECT bible_id FROM bible_books) ORDER by id", ())
		self.outputTable("Bibles without Books (and have filesets).", ["filesetId", "hashId"], resultSet)


	def filesetsWithoutCopyrights(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hash_id NOT IN" +
			" (SELECT hash_id FROM bible_fileset_copyrights) ORDER by id", ())
		self.outputTable("Filesets without Copyrights.", ["filesetId", "hashId"], resultSet)


	def filesetsWithoutOrganizations(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hash_id NOT IN" +
			" (SELECT hash_id FROM bible_fileset_copyrights) ORDER by id", ())
		self.outputTable("Filesets without Organizations.", ["filesetId", "hashId"], resultSet)


	def filesetsWithoutAccessGroups(self):
		resultSet = self.db.select("SELECT id, hash_id FROM bible_filesets WHERE hidden = 0 and hash_id NOT IN" +
			" (SELECT hash_id FROM access_group_filesets) ORDER BY id", ())

		lptsReader = LPTSExtractReader(config.filename_lpts_xml)

		finalResultSet = []
		for fileset in resultSet:
			(filesetId, hashId) = fileset

			filesetFromReader = lptsReader.getFilesetRecords(filesetId)
			if filesetFromReader != None:
				for (status, record) in filesetFromReader:
					if status == "Live":
						finalResultSet.append(fileset)

		self.outputTable("Filesets without Access Groups.", ["filesetId", "hashId"], finalResultSet)

	def booksWithPlaceholders(self):
		resultSet = self.db.select("SELECT bb.bible_id, count(*) FROM bible_books bb WHERE bb.bible_id in (SELECT DISTINCT (bfl.bibleid) FROM bible_fileset_lookup bfl WHERE bfl.type like %s) and LEFT(bb.name,1) = %s GROUP BY bb.bible_id", ('text%', '['))
		self.outputTable("Books with Placeholders ([book_name]).", ["bibleId", "num books"], resultSet)


	def totalLanguageCount(self):
		resultSet = self.db.select("SELECT count(distinct iso) FROM languages WHERE id IN (SELECT language_id FROM bibles)", ())		
		self.outputTable("Total ISO-639-3 languages:", ["count"], resultSet)


	def totalBiblesWithFilesets(self):
		resultSet = self.db.select("SELECT count(distinct bible_id) FROM bible_fileset_connections", ())
		self.outputTable("Total Bibles with Filesets:", ["count"], resultSet)


	def totalFilesetCount(self):
		resultSet = self.db.select("SELECT count(distinct hash_id) FROM bible_fileset_connections", ())
		self.outputTable("Total Filesets:", ["count"], resultSet)


	# Check for each file in bible_files to insure that it exists in the DBP
	def bibleFilesToS3(self, dbpProdModified):
		files = set()
		filesets = set()
		missingFilesetIds = set()
		absentFilesetIds = set()
		dbProd = io.open(self.config.directory_bucket_list + self.config.s3_bucket +".txt", mode="r", encoding="utf-8")
		for line in dbProd:
			if "delete" not in line:
				fullFilename = line.split("\t")[0]
				files.add(fullFilename)
				parts = fullFilename.split("/")
				if len(parts) > 3:
					fileset = "%s/%s/%s/" % (parts[0], parts[1], parts[2])
					filesets.add(fileset)
		dbProd.close()
		print(len(files), self.config.s3_bucket +" records")
		dbVid = io.open(self.config.directory_bucket_list + self.config.s3_vid_bucket +".txt", mode="r", encoding="utf-8")
		for line in dbVid:
			fullFilename = line.split("\t")[0]
			if fullFilename.endswith(".m3u8"):
				files.add(fullFilename)
				parts = fullFilename.split("/")
				if len(parts) > 3:
					fileset = "%s/%s/%s/" % (parts[0], parts[1], parts[2])
					filesets.add(fileset)
		dbVid.close()
		print(len(files), self.config.s3_bucket + " + " + self.config.s3_vid_bucket + " records")
		sql = ("SELECT f.set_type_code, c.bible_id, f.id, bf.file_name"
			" FROM bible_filesets f, bible_files bf, bible_fileset_connections c"
			" WHERE f.hash_id = bf.hash_id AND f.hash_id = c.hash_id"
			" AND bf.updated_at < %s"
			" ORDER BY f.id, c.bible_id")
		resultSet = self.db.select(sql, (dbpProdModified))
		for (typeCode, bibleId, filesetId, filename) in resultSet:
			typeCd = typeCode.split("_")[0]
			if len(filesetId) < 10 or filesetId[-2:] != "SA":
				fullKey = "%s/%s/%s/%s" % (typeCd, bibleId, filesetId, filename)
				if fullKey not in files:
					self.missingS3Objects.append((fullKey,))
					missingFilesetIds.add((typeCd, bibleId, filesetId))
				prefixKey = "%s/%s/%s/" % (typeCd, bibleId, filesetId)
				if prefixKey not in filesets:
					absentFilesetIds.add((typeCd, bibleId, filesetId))
		self.outputTable("Filesets with all files missing from s3 bucket.", ["type", "bibleId", "filesetId"], sorted(absentFilesetIds))
		self.outputTable("Filesets with some files missing from s3 bucket.", ["type", "bibleId", "filesetId"], sorted(missingFilesetIds))


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
										missing.append((damId, typeCode, index, rec.Reg_StockNumber()))
		self.outputTable("DamIds with no bible_filesets record.", ["damId", "typeCode", "index", "stockno"], sorted(missing))


	def checkForMissingTS(self):
		missing = self.db.select('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
		 						' (SELECT hash_id FROM bible_files WHERE id IN'
		 						' (SELECT bible_file_id FROM bible_file_stream_bandwidths WHERE id NOT IN'
		 						' (SELECT stream_bandwidth_id FROM bible_file_stream_ts)))', ())
		self.outputTable("FilesetId has some or all bible_file_stream_ts records missing.", ["filesetId"], sorted(missing))


	def checkForMissingBandwidth(self):
		missing = self.db.select('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
								' (SELECT hash_id FROM bible_files WHERE id NOT IN'
								' (SELECT bible_file_id FROM bible_file_stream_bandwidths))', ())
		self.outputTable("FilesetId has some or all bible_file_stream_bandwidths records missing.", ["filesetId"], sorted(missing))


	def bibleIdCheck(self):
		noFilesetInDBP = []
		bibleIdMismatch = []
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
						noFilesetInDBP.append((stockNo, damId, fieldname, status))
					elif dbpBibleId != lptsBibleId:
						bibleIdMismatch.append((stockNo, damId, fieldname, status, lptsBibleId, dbpBibleId))
		self.outputTable("Stockno / damId has no filesetId in DBP", ["stockno", "damId", "fieldname", "status"], noFilesetInDBP)
		self.outputTable("Stockno / damId has bibles, but does not match DBP", ["stockno", "damId", "fieldname", "status", "lpts_bibleId", "dbp_bibleId"], bibleIdMismatch)


	def outputTable(self, title, columns, results):
		self.htmlOut.write("<h3>%s</h3>\n" % (title,))
		self.htmlOut.write("<table>")
		self.htmlOut.write("<tr>")
		for col in columns:
			self.htmlOut.write("<th>%s</th>" % (col,))
		self.htmlOut.write("</tr>\n")
		if len(results) > 0:
			for row in results:
				self.htmlOut.write("<tr>")
				for value in row:
					self.htmlOut.write("<td>%s</td>" % (value,))
				self.htmlOut.write("</tr>\n")
		else:
			self.htmlOut.write("<tr>")
			for col in columns:
				self.htmlOut.write("<td></td>")
			self.htmlOut.write("</tr>")
		self.htmlOut.write("</table>\n")
		self.htmlOut.write("<p>Error count: %d</p>\n" % (len(results)))


if (__name__ == '__main__'):
	config = Config()
	if len(sys.argv) > 2 and sys.argv[2].lower() == "full":
		bucket = DownloadBucketList(config)
		pathname = bucket.listBucket(config.s3_bucket)
		DBPRunFilesS3.simpleUpload(config, pathname, "text/plain")
		dbpProdModified = datetime.utcnow()
		pathname = bucket.listBucket(config.s3_vid_bucket)
		DBPRunFilesS3.simpleUpload(config, pathname, "text/plain")
		dbpVidModified = dbpProdModified
	elif len(sys.argv) > 2 and sys.argv[2].lower() == "fast":
		pathname = config.directory_bucket_list + config.s3_bucket +".txt"
		DBPRunFilesS3.simpleDownload(config, pathname)
		dbpProdModified = DBPRunFilesS3.simpleLastModified(config, config.s3_bucket +".txt")
		pathname = config.directory_bucket_list + config.s3_vid_bucket +".txt"
		DBPRunFilesS3.simpleDownload(config, pathname)
		dbpVidModified = DBPRunFilesS3.simpleLastModified(config, config.s3_vid_bucket +".txt")
	elif len(sys.argv) > 2 and sys.argv[2].lower() == "restart":
		dbpProdModified = DBPRunFilesS3.simpleLastModified(config, config.s3_bucket +".txt")
		dbpVidModified = DBPRunFilesS3.simpleLastModified(config, config.s3_vid_bucket +".txt")
	else:
		print("Usage: python3 load/CompleteCheck.py  config_profile  full|fast|restart")
		sys.exit()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)
	check = CompleteCheck(config, db, lptsReader)
	check.totalLanguageCount()
	check.totalFilesetCount()
	check.totalBiblesWithFilesets()
	check.filesetsWithoutConnects()
	check.filesetsWithoutFiles()
	check.filesetsWithoutVerses()
	check.biblesWithoutTitles()
	check.biblesWithoutBooks()
	check.filesetsWithoutCopyrights()
	check.filesetsWithoutOrganizations()
	check.filesetsWithoutAccessGroups()
	check.bibleFilesToS3(dbpProdModified)
	check.bibleFilesetsToLPTS()
	check.checkForMissingTS()
	check.checkForMissingBandwidth()
	check.bibleIdCheck()
	check.biblesWithoutConnects()
	check.booksWithPlaceholders()
	check.close()
	db.close()
	DBPRunFilesS3.simpleUpload(config, CompleteCheck.HTML_FILE, "text/html")



