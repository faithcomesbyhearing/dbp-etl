# OneTimeUpdateDBSWEB.py


import io
import os
import sys
from datetime import datetime
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class BibleFilesetRecord:

	def __init__(self, hashId, numFiles):
		self.hashId = hashId
		self.numFiles = numFiles if numFiles > 1 else 0
		self.numVerses = 0

	def toString(self):
		return "%s, %d, %d" % (self.hashId, self.numFiles, self.numVerses)

class BibleFileset:

	def __init__(self, resultSet):
		self.fcbhFormat = None
		self.fcbhPlain = None
		self.dbsFormat = None
		self.dbsPlain = None
		self.prodFiles = 0
		for row in resultSet:
			record = BibleFilesetRecord(row[2], row[3])
			if row[0] == "dbp-prod" and row[1] == "text_format":
				self.fcbhFormat = record
			elif row[0] == "dbp-prod" and row[1] == "text_plain":
				self.fcbhPlain = record
			elif row[0] == "dbs-web" and row[1] == "text_format":
				self.dbsFormat = record
			elif row[0] == "dbs-web" and row[1] == "text_plain":
				self.dbsPlain = record

#	def hasContent(self):
#		return (self.fcbhFormat != None or
#				self.fcbhPlain != None or
#				self.dbsFormat != None or
#				self.dbsPlain != None or
#				self.prodFiles > 0)

	def hasDBSWEBContent(self):
		return self.dbsFormat != None or self.dbsPlain != None

	def toString(self):
		result = []
		if self.fcbhFormat != None:
			result.append("%s: %s" % ("fcbh-format", self.fcbhFormat.toString()))
		if self.fcbhPlain != None:
			result.append("%s: %s" % ("fcbh-plain", self.fcbhPlain.toString()))
		if self.dbsFormat != None:
			result.append("%s: %s" % ("dbs-format", self.dbsFormat.toString()))
		if self.dbsPlain != None:
			result.append("%s: %s" % ("dbs-plain", self.dbsPlain.toString()))
		#if self.prodFiles > 0:
		result.append("dbp-prod: %d" % (self.prodFiles))
		return "\n".join(result)


class OneTimeUpdateDBSWEB:

	def __init__(self):
		self.config = Config()
		self.db = SQLUtility(self.config)


	def close(self):
		self.db.close()


	def process(self):
		dbpProdMap = self.getDBPProdFiles()
		allFilesets = []
		for (bibleId, filesetId) in self.getBibleFilesetList():
			#print("1", bibleId, filesetId)
			filesetList = self.getFileset(bibleId, filesetId)
			filesetList.prodFiles = dbpProdMap.get(bibleId + "/" + filesetId, 0)
			if filesetList.hasDBSWEBContent():
				print("2", bibleId, filesetId)
				print(filesetList.toString())
				self.processTextFormat(filesetList)
			#allFilesets.append(getFileset(bibleId, filesetId))
			# populate with verse information

		
	def getDBPProdFiles(self):
		fileCountMap = {}
		#print(self.config.directory_bucket_list + "dbp-prod.txt")
		fp = open(self.config.directory_bucket_list + "dbp-prod.txt")
		for line in fp:
			if line.startswith("text/"):
				#print(line)
				parts = line.split("/")
				if len(parts) > 3 and len(parts[1]) < 20 and len(parts[2]) < 20:
					key = parts[1] + "/" + parts[2]
					#print(key)
					count = fileCountMap.get(key, 0)
					count += 1
					fileCountMap[key] = count
		#for fileset in sorted(fileCountMap.keys()):
		#	count = fileCountMap[fileset]
		#	print(fileset, count)
		fp.close()
		return fileCountMap

	def getBibleFilesetList(self):
		sql = ("SELECT distinct bfc.bible_id, bs.id"
			" FROM bible_filesets bs"
			" JOIN bible_fileset_connections bfc ON bfc.hash_id = bs.hash_id")
		resultSet = self.db.select(sql, ())
		return resultSet


	def getFileset(self, bibleId, filesetId):
		sql = ("SELECT bs.asset_id, bs.set_type_code, bs.hash_id, count(*)" 
			" FROM bible_filesets bs"
			" JOIN bible_fileset_connections bfc ON bfc.hash_id=bs.hash_id"
			" LEFT JOIN bible_files bf ON bs.hash_id=bf.hash_id"
			" WHERE bfc.bible_id = %s"
			" AND bs.id = %s"
			" GROUP BY bs.asset_id, bs.set_type_code, bs.hash_id")
		resultSet = self.db.select(sql, (bibleId, filesetId))
		#for row in resultSet:
		#	print(row)
		return BibleFileset(resultSet)


	def getVerses(self, hashId):
		sql = ("SELECT count(*) FROM bible_verses WHERE hash_id = %s")
		count = self.db.selectScalar(sql, (hashId))
		return count


	def processTextFormat(self, fileset):
		if fileset.dbsFormat != None:
			if fileset.dbsFormat.numFiles > 10 and fileset.prodFiles > 10:
				if fileset.fcbhFormat == None:
					print("CHANGE %s dbs-web text_format to dbp-prod" % (fileset.dbsFormat.hashId))
				elif fileset.fcbhFormat != None and fileset.fcbhFormat.numFiles == 0:
					print("DELETE %s dbp-prod text_format" % (fileset.fcbhFormat.hashId))
					print("CHANGE %s dbs-web text_format to dbp-prod" % (fileset.dbsFormat.hashId))
			else:
				print("DELETE %s dbs-web text_format has no files in dbp-prod." % (fileset.dbsFormat.hashId))
			if fileset.dbsFormat.numVerses > 100:
				print("WARN: %s dbs-web text_format has %d verses." % (fileset.dbsFormat.hashId, fileset.dbsFormat.numFiles))

	"""
	dbs-web 1647
	dbp-prod 5712
	dbp-vid 411

	text_plain 2292
	text_format 1406

	dbp-prod text_format 586
	dbp-prod text_plain 1465
	dbs-web text_format 820
	dbs-web text_plain 827

	There are no text filesets that do not have a bible_id

	There are no filesets that have more than one bible_id

	There are no bible_verses that have a set_type_code of text_format

	1) build a structure that is (hash_id, bible_id, fileset_id, asset_id, set_type_code, num_files, num_verses)
	2) have as 4 groups dbp-prod, text_plain, dbp-prod, text_format, dbs-web, text_plain, dbs-web, text_format

	Decision Box
	conditions:
	has files, per DBP
	has verses, per DBP
	has files, per dbp-prod

	A. Iterate over each bible/fileset pair in DBP
	B. Put data info an object:
		dbs-web/text_format:
		dbp-prod/text_format:
		dbs-web/text_plain:
		dbp-prod/text_plain:
		num_dbp_prod_files:
		each carries a record: (hash_id, num_verses, num_files)
	C. Iterate over DBP-PROD to populate num_files field
	D. This logic ignores things in bucket, but no DBP, which is OK. That will be solved later.

	Process text_format
	1) if dbs-web exists and has files and dbp-prod does not exist -> change to dbp-prod
	2) if dbs-web exists and has files and dbp-prod exists, but no files -> change dbs-web to dbp-prod
	3) if dbs-web exists and has no files -> report error
	4) if dbs-web exists and has verses -> report error

	Process text_plain
	1) if dbs-web exists and has verses and dbp-prod does not exist, and text_format exists -> change to dbp-prod
	2) if dbs-web exists and has verses and dbp-prod exists, but has no verses, and text_format exists -> change to dbp-prod
	3) if dbs-web exists and has no verses -> report error
	3) if dbs-web exists and has files -> report error
	"""

if (__name__ == '__main__'):
	once = OneTimeUpdateDBSWEB()
	fsMap = once.process()
	#print(fsMap)


