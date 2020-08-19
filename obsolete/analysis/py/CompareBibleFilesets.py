# CompareBibleFilesets.py

# This program compares the development bible_filesets table with the dbp database
# and produces a list of differences for the purposes of debugging the new load program.

# Exclude text_plain from production dbp at this time.
# Using filesetId as the common key, find 3 sets 
# a. those in both; b. this in prod, not dev; c. those in dev not prod
# Report sets b and c
# For those in both, compare contents between rows in dbp and dev that match
# However, ignore hash_id because of incorrect dbs-prod entries in production
# And treat dbs-prod as dbp-prod

import os
import io
import sys
import re
from Config import *
from SQLUtility import *


class CompareBibleFilesets:

	def __init__(self, config):
		self.db = SQLUtility("localhost", 3306, "root", "valid_dbp")


	def compareNotInTest(self):
		self.quarantine = os.listdir(config.directory_quarantine)
		self.bucket = []
		fp = open(config.directory_bucket_list + os.sep + "dbp-prod.txt", "r")
		for line in fp:
			if "delete" not in line:
				self.bucket.append(line)
		fp.close()
		fp = open(config.directory_bucket_list + os.sep + "dbp-vid.txt", "r")
		for line in fp:
			if line.strip().endswith(".mp4"):
				self.bucket.append(line)
		fp.close()

		notInBucket = []
		hasRowsInBucket = []
		hasSomethingInBucket = []
		inQuarantine = []
		inDbsWeb = []
		sql = ("SELECT id, asset_id, set_type_code, set_size_code, hidden"
			" FROM dbp.bible_filesets WHERE set_type_code NOT IN ('text_plain', 'app')"
			" AND id NOT IN (SELECT id FROM valid_dbp.bible_filesets)"
			" ORDER BY asset_id, set_type_code, id")
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			typeCode = row[2].split("_")[0]
			filesetId = row[0]
			if self.checkQuarantine(typeCode, filesetId):
				inQuarantine.append(row)
			else:
				count = self.checkBucket(typeCode, filesetId)
				if count > 0:
					hasRowsInBucket.append((row, count))
				else:
					count = self.checkBucketApprox(filesetId)
					if count > 0:
						hasSomethingInBucket.append((row, count))
					elif row[1] == "dbs-web":
						inDbsWeb.append(row)
					else:
						notInBucket.append(row)

		self.reportErrors("Are not found in bucket.", notInBucket)
		self.reportErrors("Are in dbs-web.", inDbsWeb)
		self.reportErrors("Have some rows in bucket.", hasRowsInBucket)
		self.reportErrors("Are in bucket, but not in filesetId position.", hasSomethingInBucket)
		self.reportErrors("Are in Quarantine.", inQuarantine)

		print("%d differences found" % (len(resultSet)))


	def checkQuarantine(self, typeCode, filesetId):
		pattern = re.compile(r"[a-z\-]+_" + typeCode + "_[A-Z0-9]+_" + filesetId + ".csv")
		for filename in self.quarantine:
			if pattern.match(filename) != None:
				return True
		return False


	def checkBucket(self, typeCode, filesetId):
		pattern = re.compile(typeCode + "/[A-Z0-9]+/" + filesetId + "/.+")
		typeCodeSrch = typeCode + "/"
		filesetIdSrch = "/" + filesetId + "/"
		count = 0
		for filename in self.bucket:
			if pattern.match(filename) != None:
				count += 1
		return count


	def checkBucketApprox(self, filesetId):
		search = "/" + filesetId + "/"
		count = 0
		for filename in self.bucket:
			if search in filename:
				count += 1
		return count


	def reportErrors(self, message, rows):
		print("%d %s" % (len(rows), message))
		for row in rows:
			if len(row) == 2:
				fileset = row[0]
				print("  ", row[1], '\t', fileset[0], fileset[1], fileset[2], fileset[3])
			else:
				print(" ", row[0], row[1], row[2], row[3])
		print("")


	def compareNotInProd(self):
		sql = ("SELECT id, asset_id, set_type_code, set_size_code, hidden"
			" FROM valid_dbp.bible_filesets WHERE id NOT IN (SELECT id FROM dbp.bible_filesets)"
			" ORDER BY asset_id, set_type_code, id")
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			print(row)
		print(len(resultSet), "found")


	def compareCommon(self):
		bucketErrors = []
		typeCodeErrors = []
		hashIdErrors = []
		sizeCodeErrors = []
		secondRowErrors = []
		sql = ("SELECT t.id, t.asset_id, t.set_type_code, t.set_size_code, t.hash_id,"
			" p.asset_id, p.set_type_code, p.set_size_code, p.hash_id"
			" FROM valid_dbp.bible_filesets t, dbp.bible_filesets p"
			" WHERE t.id = p.id"
			" AND p.set_type_code NOT IN ('text_plain', 'app')"
			" ORDER BY t.id, p.asset_id, p.set_type_code")
		resultSet = self.db.select(sql, ())
		filesetIdMap = {}
		for row in resultSet:
			filesetId = row[0]
			filesets = filesetIdMap.get(filesetId, [])
			filesets.append(row)
			filesetIdMap[filesetId] = filesets
		print(len(resultSet), "found")
		for filesetId, rows in filesetIdMap.items():
			row = rows[0]
			testBucket = row[1]
			testTypeCode = row[2]
			testSizeCode = row[3]
			testHashId = row[4]
			prodBucket = row[5]
			prodTypeCode = row[6]
			prodSizeCode = row[7]
			prodHashId = row[8]
			if testBucket != prodBucket:
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					bucketErrors.append(row)
			if testTypeCode != prodTypeCode:
				typeCodeErrors.append(row)
			if testHashId != prodHashId:
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					hashIdErrors.append(row)
			if testSizeCode != prodSizeCode:
				sizeCodeErrors.append(row)

			if len(rows) == 2:
				row = rows[1]
				testBucket = row[1]
				prodBucket = row[5]
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					secondRowErrors.append(row)

			if len(rows) > 2:
				print("ERRORS")
				print(filesetId)
				for row in rows:
					print(row)
				sys.exit()

		self.reportErrors2("Bucket Errors", bucketErrors)
		self.reportErrors2("SetTypeCode Errors", typeCodeErrors)
		self.reportErrors2("HashId Errors", hashIdErrors)
		self.reportErrors2("SetSizeCode Errors", sizeCodeErrors)
		#self.reportErrors2("Second Row Errors", secondRowErrors) # same as setTypeCode Errors
		print("%d filesetid matches found" % (len(filesetIdMap.keys())))


	def reportErrors2(self, message, rows):
		print("%d %s" % (len(rows), message))
		for row in rows:
			if len(row) == 2:
				fileset = row[0]
				print("  ", row[1], '\t', fileset[0], fileset[1], fileset[2], fileset[3])
			else:
				#print(" ", row[0], row[1], row[2], row[3])
				print(row)
		print("")

"""
Use this later when I have a bible_id
	def reportSizeCode(self, message, rows):
		csvFilename = "%s/%s_%s_%s_%s.csv" % (self.config.directory_accepted, bucket, typeCode, bibleId, filesetId)
		print("%d %s" % (len(rows), message))
		for row in rows:
			bookIdSet = set()
			with open(csvFilename, newline='\n') as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					bookIdSet.add(row["book_id"])
			hasOT = len(bookIdSet.intersection(self.OT))
			hasNT = len(bookIdSet.intersection(self.NT))			
"""

config = Config("dev")
compare = CompareBibleFilesets(config)
#compare.compareNotInTest()
#compare.compareNotInProd()
compare.compareCommon()
