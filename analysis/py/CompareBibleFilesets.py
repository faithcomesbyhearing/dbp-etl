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


	def compareNotInTest(self):
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

		self.reportErrors("Not found in bucket", notInBucket)
		self.reportErrors("Has some rows in bucket", hasRowsInBucket)
		self.reportErrors("In bucket, but not in filesetId position", hasSomethingInBucket)
		self.reportErrors("Is in Quarantine", inQuarantine)
		self.reportErrors("Is in dbs-web", inDbsWeb)
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
		print(message)
		for row in rows:
			if len(row) == 2:
				fileset = row[0]
				print("  ", row[1], '\t', fileset[0], fileset[1], fileset[2], fileset[3])
			else:
				print(" ", row[0], row[1], row[2], row[3])
		print("")



config = Config("dev")
compare = CompareBibleFilesets(config)
compare.compareNotInTest()
