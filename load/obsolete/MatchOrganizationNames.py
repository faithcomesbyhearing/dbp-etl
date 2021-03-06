# MatchOrganizationNames.py
#
# This is a program used in developement, not production
# It matches existing organization names to licensors to produce a list of those
# that match and those that do not.

import io
import os
import sys
import csv
import operator
from datetime import datetime
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class MatchOrganizationNames:

	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader
		self.licensorUpdates = set()
		self.copyrightInserts = set()

		self.licensorsNoMatch = set()
		self.copyHolderNoMatch = set()
		self.organizationMap = {}
		#resultSet = self.db.select("select id, slug from organizations where id in (select organization_id from bible_fileset_copyright_organizations) order by slug", ())
		resultSet = self.db.select("SELECT id, slug from organizations order by slug", ())
		for row in resultSet:
			self.organizationMap[int(row[0])] = (int(row[0]), row[1], set(), set())
		#with open("/Volumes/FCBH/database_data/organizations.csv", newline="\n") as csvfile:
		#	reader = csv.reader(csvfile, delimiter=',', quotechar='"')
		#	for row in reader:
		#		self.organizationMap[int(row[0])] = (int(row[0]), row[1], set(), set())
		self.langMap = {}
		#self.langMap = self.db.selectMap("SELECT name, organization_id FROM organization_translations where organization_id in (select organization_id from bible_fileset_copyright_organizations) order by name", ())
		resultSet = self.db.select("SELECT name, organization_id FROM organization_translations order by name", ())
		for row in resultSet:
			self.langMap[row[0]] = int(row[1])
		#with open("/Volumes/FCBH/database_data/organization_translations.csv", newline="\n") as csvfile:
		#	reader = csv.reader(csvfile, delimiter=',', quotechar='"')
		#	for row in reader:
		#		self.langMap[row[0]] = int(row[1])


	def hasDamIds(self, lptsRecord, typeCode):
		damIds = lptsRecord.DamIds(typeCode, 1)
		if len(damIds) > 0:
			return True
		damIds = lptsRecord.DamIds(typeCode, 2)
		if len(damIds) > 0:
			return True
		damIds = lptsRecord.DamIds(typeCode, 3)
		if len(damIds) > 0:
			return True
		return False


	def addTables(self):
		sql = ("CREATE TABLE IF NOT EXISTS lpts_organizations"
			" (lpts_organization varchar(256) NOT NULL,"
			" organization_role int(1) unsigned NOT NULL CHECK (organization_role in (1, 2)),"
			" organization_id int(10) unsigned NOT NULL,"
			" created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
  			" updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
			" FOREIGN KEY (organization_id) REFERENCES organizations(id))"
			" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
		self.db.execute(sql, ())
		self.db.execute("TRUNCATE TABLE lpts_organizations", ())


	def processAlansLicensorMatch(self):
		with open(self.config.directory_bucket_list + "/alan_licensors.csv", newline="\n") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				orgId = int(row[0])
				if len(row[2]) > 0:
					licensor = row[1] + "," + row[2]
				else:
					licensor = row[1]
				self.licensorUpdates.add((licensor, str(orgId)))
		#print(self.licensorUpdates)


	def processAlansCopyrightMatch(self):
		with open(self.config.directory_bucket_list + "/alan_copyright_holders.csv", newline="\n", encoding='utf-16') as csvfile:
			reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
			for row in reader:
				if row[0] != None and row[0] != "" and row[0] != "?":
					self.copyrightInserts.add((row[3], int(row[0])))
				if row[1] != None and row[1] != "" and row[1] != "?":
					self.copyrightInserts.add((row[3], int(row[1])))
				if row[2] != None and row[2] != "" and row[2] != "?":
					orgId = row[2]
					if orgId == '755':
						orgId = '224'
					self.copyrightInserts.add((row[3], int(orgId)))
		#print(self.copyrightInserts)


	def processLicensor(self):
		for record in self.lptsReader.resultSet:
			if self.hasDamIds(record, "text"):
				if record.Licensor() != None:
					self.matchLicensor(record.Licensor())
				if record.CoLicensor() != None:
					self.matchLicensor(record.CoLicensor())

		print(len(self.licensorsNoMatch), "Licensors that do not match")
		count = 0
		for (orgId, slug, licensors, copyHolders) in self.organizationMap.values():
			if len(licensors) > 0:
				count += 1
			if len(licensors) > 1:
				print("Duplicate Licensor", orgId, slug, licensors)
		print(count, "Licensors that do match")


	def matchLicensor(self, licensor):
		orgId = self.langMap.get(licensor)
		if orgId != None:
			organization = self.organizationMap[orgId]
			(orgId, slug, licensors, copyHolders) = organization
			licensors.add(licensor)
			self.organizationMap[orgId] = (orgId, slug, licensors, copyHolders)
			## This adds to the organization table update
			self.licensorUpdates.add((licensor, str(orgId)))
		else:
			self.licensorsNoMatch.add(licensor)


	def processCopyHolder(self):
		for record in self.lptsReader.resultSet:
			if record.Copyrightc() != None and self.hasDamIds(record, "text"):
				self.matchCopyHolder(record.Copyrightc())
			if record.Copyrightp() != None and self.hasDamIds(record, "audio"):
				self.matchCopyHolder(record.Copyrightp())
			if record.Copyright_Video() != None and self.hasDamIds(record, "video"):
				self.matchCopyHolder(record.Copyright_Video())

		for item in self.copyHolderNoMatch:
			print("|" + item + "|")
		print(len(self.copyHolderNoMatch), "Copyright Holders that do not match")
		count = 0
		for (orgId, slug, licensors, copyHolders) in self.organizationMap.values():
			if len(copyHolders) > 0:
				count += 1
			if len(copyHolders) > 1:
				print("Duplicate CopyHolder", orgId, slug, copyHolders)
		print(count, "Copyright Holders that do match")	


	def matchCopyHolder(self, copyHolder):
		copyright = self.lptsReader.reduceCopyrightToName(copyHolder)
		orgId = self.langMap.get(copyright)
		if orgId != None:
			organization = self.organizationMap[orgId]
			(orgId, slug, licensors, copyHolders) = organization
			copyHolders.add(copyright)
			self.organizationMap[orgId] = (orgId, slug, licensors, copyHolders)
			## This adds the copyright to the table insert
			self.copyrightInserts.add((copyright, str(orgId)))
		else:
			self.copyHolderNoMatch.add(copyright)


	def displayOutput(self):
		orgTuples = self.organizationMap.values()
		organizations = sorted(orgTuples, key=operator.itemgetter(1))
		with open("organizations_with_match.csv", "w", newline="\n") as csvfile:
			writer = csv.writer(csvfile, delimiter=",", quotechar='"')
			for (orgId, slug, licensors, copyHolders) in organizations:
				licensor = "\n".join(licensors)
				copyHolder = "\n".join(copyHolders)
				writer.writerow([orgId, slug, licensor, copyHolder])

		sortedLicensors = sorted(self.licensorsNoMatch)
		fp = open("licensors_no_match.csv", "w")
		for item in sortedLicensors:
			fp.write(item + "\n")
		fp.close()

		sortedCopyHolders = sorted(self.copyHolderNoMatch)
		fp = open("copyright_holders_no_match.csv", "w")
		for item in sortedCopyHolders:
			fp.write(item + "\n")
		fp.close()


	def updateDatabase(self):
		errorCount1 = 0
		errorCount2 = 0
		finalLicensors = []
		for (orgName, orgId) in self.licensorUpdates:
			count1 = self.db.selectScalar("SELECT count(*) FROM organizations WHERE id=%s", (orgId,))
			if count1 > 0:
				finalLicensors.append((orgName, orgId))
			else:
				print("Organization does not exist for ", orgName, orgId)
				errorCount1 += 1

		finalCopyHolders = []
		for (orgName, orgId) in self.copyrightInserts:
			count = self.db.selectScalar("SELECT count(*) FROM organizations WHERE id=%s", (orgId,))
			if count > 0:
				finalCopyHolders.append((orgName, orgId))
			else:
				print("Organization does not exist for ", orgName, orgId)
				errorCount2 += 1

		if errorCount1 > 0 or errorCount2 > 0:
			print("Add missing organizations and try again")
			sys.exit()
		sql = "INSERT INTO lpts_organizations (lpts_organization, organization_role, organization_id) VALUES (%s, 2, %s)"
		self.db.executeBatch(sql, finalLicensors)
		sql = "INSERT INTO lpts_organizations (lpts_organization, organization_role, organization_id) VALUES (%s, 1, %s)"
		self.db.executeBatch(sql, finalCopyHolders)


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	test = MatchOrganizationNames(config, db, lptsReader)
	test.addTables()
	test.processAlansLicensorMatch()
	test.processAlansCopyrightMatch()
	test.processLicensor()
	test.processCopyHolder()
	test.displayOutput()
	test.updateDatabase()
	db.close()

