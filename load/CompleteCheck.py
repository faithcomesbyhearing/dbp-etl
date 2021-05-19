# CompleteCheck.py

# This program performs completeness checks of the DBP database.

# Before running this program, one must a current copy of dbp-prod.txt, and dbp-vid.txt
# These are generated by DownloadBucketList.py

import io
from Config import *
from SQLUtility import *
from LPTSExtractReader import *
from DownloadBucketList import *


class CompleteCheck:


	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader


	# Check for each file in bible_files to insure that it exists in the DBP
	def bibleFilesToS3(self):
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
			" ORDER BY f.id, c.bible_id")
		resultSet = self.db.select(sql, ())
		for (typeCode, bibleId, filesetId, filename) in resultSet:
			if len(filesetId) < 10 or filesetId[-2:] != "SA":
				#print(typeCode, bibleId, filesetId, filename)
				fullKey = "%s/%s/%s/%s" % (typeCode.split("_")[0], bibleId, filesetId, filename)
				if fullKey not in files:
					print("%s NOT in s3 bucket" % (fullKey))
					missingFilesetIds.add(filesetId)
				prefixKey = "%s/%s/%s/" % (typeCode.split("_")[0], bibleId, filesetId)
				if prefixKey not in filesets:
					absentFilesetIds.add(filesetId)
		for filesetId in sorted(missingFilesetIds):
			if filesetId not in absentFilesetIds:
				print("%s Has some files missing from s3 bucket" % (filesetId))
		for filesetId in sorted(absentFilesetIds):
			print("%s Has all files missing from s3 bucket." % (filesetId))


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
										#print(rec.Reg_StockNumber(), typeCode, index, damId, "Has no bible_filesets record.")
		for missed in sorted(missing):
			print(missed)


	def checkForMissingTS(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
		 						' (SELECT hash_id FROM bible_files WHERE id IN'
		 						' (SELECT bible_file_id FROM bible_file_stream_bandwidths WHERE id NOT IN'
		 						' (SELECT stream_bandwidth_id FROM bible_file_stream_ts)))', ())
		for missed in sorted(missing):
			print("%s Has some or all bible_file_stream_ts records missing" % (missed,))


	def checkForMissingBandwidth(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code="video_stream" AND hash_id IN'
								' (SELECT hash_id FROM bible_files WHERE id NOT IN'
								' (SELECT bible_file_id FROM bible_file_stream_bandwidths))', ())
		for missed in sorted(missing):
			print("%s Has some or all bible_file_stream_bandwidths records missing" % (missed,))


	def checkForMissingBibleFiles(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code NOT IN'
								' ("text_plain") AND hash_id NOT IN (SELECT hash_id FROM bible_files)', ())
		for missed in sorted(missing):
			print("%s Has some or all bible_files records missing" % (missed,))


	def checkForMissingBibleVerses(self):
		missing = self.db.selectList('SELECT id FROM bible_filesets WHERE set_type_code IN ("text_plain")'
							' AND hash_id NOT IN (SELECT hash_id FROM bible_verses)', ())
		for missed in sorted(missing):
			print("%s Has some or all bible_verses records missing" % (missed,))


if (__name__ == '__main__'):
	print("WARNING: This program outputs to the console.  You may want to redirect to a file using > filename")
	config = Config()
	if len(sys.argv) > 2 and sys.argv[2].lower().startswith("re"):
		retry = True
	else:
		retry = False
	if not retry:
		bucket = DownloadBucketList(config)
		bucket.listBucket('dbp-prod')
		bucket.listBucket('dbp-vid')
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)
	check = CompleteCheck(config, db, lptsReader)
	check.bibleFilesToS3()
	check.bibleFilesetsToLPTS()
	check.checkForMissingTS()
	check.checkForMissingBandwidth()
	check.checkForMissingBibleFiles()
	check.checkForMissingBibleVerses()
	db.close()

# python3 load/CompleteCheck.py test retry
# python3 load/CompleteCheck.py newdata > complete.out

