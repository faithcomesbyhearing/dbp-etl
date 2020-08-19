# OneTimeUpdateDBSWEB.py


import io
import os
import sys
import hashlib
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
		self.dbpFormatDelete = False
		self.dbpPlainDelete = False
		self.dbsFormat2fcbhFormat = False
		self.dbsPlain2fcbhPlain = False
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
		self.stockNo = None

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
		result.append("dbp-prod: %d" % (self.prodFiles))
		return "\n".join(result)


class OneTimeUpdateDBSWEB:

	def __init__(self):
		self.config = Config()
		self.db = SQLUtility(self.config)
		self.lptsReader = LPTSExtractReader(self.config)


	def close(self):
		self.db.close()


	def process(self):
		count = 0
		dbpProdMap = self.getDBPProdFiles()
		for (bibleId, filesetId) in self.getBibleFilesetList():
			filesetList = self.getFileset(bibleId, filesetId)
			filesetList.prodFiles = dbpProdMap.get(bibleId + "/" + filesetId, 0)
			if filesetList.hasDBSWEBContent():
				print(bibleId, filesetId)
				self.getNumVerses(filesetList)
				print(filesetList.toString())
				self.processTextFormat(bibleId, filesetId, filesetList)
				self.processTextPlain(bibleId, filesetId, filesetList)
				#self.updateFilesetGroup(bibleId, filesetId, filesetList)
				count += 1
				#if count > 100:
				#	break


		
	def getDBPProdFiles(self):
		fileCountMap = {}
		fp = open(self.config.directory_bucket_list + "dbp-prod.txt")
		for line in fp:
			if line.startswith("text/"):
				parts = line.split("/")
				if len(parts) > 3 and len(parts[1]) < 20 and len(parts[2]) < 20:
					key = parts[1] + "/" + parts[2]
					count = fileCountMap.get(key, 0)
					count += 1
					fileCountMap[key] = count
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
		bibleFileset = BibleFileset(resultSet)
		(lptsRecord, index) = self.lptsReader.getLPTSRecordLoose("text", bibleId, filesetId)
		if lptsRecord != None:
			bibleFileset.stockNo = lptsRecord.Reg_StockNumber()
		return bibleFileset


	def getNumVerses(self, fileset):
		sql = "SELECT count(*) FROM bible_verses WHERE hash_id = %s"
		if fileset.dbsFormat != None:
			fileset.dbsFormat.numVerses = self.db.selectScalar(sql, (fileset.dbsFormat.hashId))
		if fileset.dbsPlain != None:
			fileset.dbsPlain.numVerses = self.db.selectScalar(sql, (fileset.dbsPlain.hashId))
		if fileset.fcbhFormat != None:
			fileset.fcbhFormat.numVerses = self.db.selectScalar(sql, (fileset.fcbhFormat.hashId))
		if fileset.fcbhPlain != None:
			fileset.fcbhPlain.numVerses = self.db.selectScalar(sql, (fileset.fcbhPlain.hashId))

	def processTextFormat(self, bibleId, filesetId, fileset):
		if fileset.dbsFormat != None:
			hashId = fileset.dbsFormat.hashId
			if fileset.dbsFormat.numFiles > 10 and fileset.prodFiles > 10:
				if fileset.fcbhFormat == None:
					fileset.dbsFormat2fcbhFormat = True
					if fileset.stockNo != None:
						print("WARN1 %s %s/%s dbs-web has LPTS record %s and %d files in dbp-prod." % (hashId, bibleId, filesetId, fileset.stockNo, fileset.dbsFormat.numFiles))
					else:
						print("WARN2 %s %s/%s dbs-web has %d files in dbp-prod, but no LPTS record." % (hashId, bibleId, filesetId, fileset.dbsFormat.numFiles))
				elif fileset.fcbhFormat != None and (fileset.fcbhFormat.numFiles == 0 or fileset.prodFiles == 0):
					fileset.dbpFormatDelete = True
					fileset.dbsFormat2fcbhFormat = True
					#print("DELETE2 %s %s/%s dbp-prod text_format" % (fileset.fcbhFormat.hashId, bibleId, filesetId))
					print("WARN99 %s %s/%s dbs-web has files in dbp-prod and identical dbp-prod fileset with no files." % (hashId, bibleId, filesetId))
			else:
				if fileset.stockNo != None:
					print("WARN3 %s %s/%s dbs-web LPTS record %s, but no files in dbp-prod." % (hashId, bibleId, filesetId, fileset.stockNo))
				else:
					print("WARN4: %s %s/%s dbs-web text_format has no files in dbp-prod and no LPTS record." % (hashId, bibleId, filesetId))
			if fileset.dbsFormat.numVerses > 100:
				print("WARN98: %s %s/%s dbs-web text_format has %d verses." % (hashId, fileset.dbsFormat.numFiles, bibleId, filesetId))


	def processTextPlain(self, bibleId, filesetId, fileset):
		if fileset.dbsPlain != None:
			hashId = fileset.dbsPlain.hashId
			if fileset.fcbhFormat != None or fileset.dbsFormat2fcbhFormat:
				if fileset.dbsPlain.numVerses > 100:
					if fileset.fcbhPlain == None:
						fileset.dbsPlain2fcbhPlain = True
						print("CHANGE3 %s %s/%s dbs-web text_plain to db" % (hashId, bibleId, filesetId))
					elif fileset.fcbhPlain != None and fileset.fcbhPlain.numVerses == 0:
						fileset.dbpPlainDelete = True
						fileset.dbsPlain2fcbhPlain = True
						print("DELETE4 %s %s/%s dbp-prod text_plain" % (fileset.fcbhFormat.hashId, bibleId, filesetId))
						print("CHANGE3 %s %s/%s dbs-web text_plain to db" % (hashId, bibleId, filesetId))
			if fileset.dbsPlain.numVerses == 0:
				print("WARn: %s %s/%s dbs-web text_plain has no verses" % (hashId, bibleId, filesetId))
			if fileset.dbsPlain.numFiles > 0:
				print("WARn: %s %s/%s dbs-web text_plain has %d files" % (hashId, bibleId, filesetId, fileset.dbsPlain.numFiles))


	def updateFilesetGroup(self, bibleId, filesetId, fileset):
		self.statements = []
		if fileset.dbpFormatDelete:
			self.deleteFileset(bibleId, filesetId, "text_format", fileset)
		if fileset.dbpPlainDelete:
			self.deleteFileset(bibleId, filesetId, "text_plain", fileset)
		if fileset.dbsFormat2fcbhFormat:
			self.replaceFileset(bibleId, filesetId, "text_format", fileset.dbsFormat)
		if fileset.dbsPlain2fcbhPlain:
			self.replaceFileset(bibleId, filesetId, "text_plain", fileset.dbsPlain)
		#self.db.executeTransaction(self.statements)
		self.db.displayTransaction(self.statements)


	def deleteFileset(self, bibleId, filesetId, typeCode, fileset):
		stmt = "DELETE bible_fileset WHERE hash_id = %s"
		self.statements.append((stmt, [(fileset.hashId)]))


	def replaceFileset(self, bibleId, filesetId, typeCode, fileset):
		bucket = "db" if typeCode == "text_plain" else "dbp-prod"
		newHashId = self.getHashId(bucket, filesetId, typeCode)
		stmt = "UPDATE bible_filesets SET hash_id = %s, asset_id = %s WHERE hash_id = %s"
		self.statements.append((stmt, [(newHashId, bucket, fileset.hashId)]))
		tableNames = ("access_group_filesets", "bible_fileset_connections",
			"bible_fileset_tags", "bible_fileset_copyrights", "bible_fileset_copyright_organizations",
			"bible_files", "bible_verses")
		for table in tableNames:
			stmt = "UPDATE %s" % (table) + " SET hash_id = %s WHERE hash_id = %s" 
			self.statements.append((stmt, [(newHashId, fileset.hashId)]))



		#attrNames = ("hash_id", "id", "asset_id", "set_type_code", "set_size_code", "hidden", "created_at", "updated_at")
		#attrNames = ("hash_id", "access_group_id", "created_at", "updated_at")
		#attrNames = ("hash_id", "bible_id", "created_at", "updated_at")
		#attrNames = ("hash_id", "name", "description", "admin_only", "notes", "iso", "language_id", "created_at", "updated_at")
		#attrNames = ("hash_id", "copyright_date", "copyright", "copyright_description", "created_at", "updated_at", "open_access")
		#attrNames = ("hash_id", "organization_id", "organization_role", "created_at", "updated_at")
		#attrNames = ("hash_id", "book_id", "chapter_start", "chapter_end", "verse_start", "verse_end", 
		#	"file_name", "file_size", "duration", "created_at", "updated_at")
		#attrNames = ("hash_id", "book_id", "chapter", "verse_start", "verse_end", "verse_text")

	def getHashId(self, bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]

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


