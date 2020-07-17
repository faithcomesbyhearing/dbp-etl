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
		#SELECT id, slug from organizations order by slug"
		with open("/Volumes/FCBH/database_data/organizations.csv", newline="\n") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				self.organizationMap[int(row[0])] = (int(row[0]), row[1], set(), set())
		self.langMap = {}
		#self.langMap = self.db.selectMap("SELECT name, organization_id FROM organization_translations where organization_id in (select organization_id from bible_fileset_copyright_organizations) order by name", ())
		#SELECT name, organization_id FROM organization_translations order by name
		with open("/Volumes/FCBH/database_data/organization_translations.csv", newline="\n") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				self.langMap[row[0]] = int(row[1])


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


	def addColumns(self):
		sql = ("SELECT count(*) FROM information_schema.columns"
			" where table_name='organizations' and column_name='lpts_licensor'")
		num = self.db.selectScalar(sql, ())
		if num == 0:
			sql = "ALTER TABLE organizations ADD column lpts_licensor varchar(255) null"
			self.db.execute(sql, ())
		self.db.execute("UPDATE organizations SET lpts_licensor = NULL", ())
		sql = ("CREATE TABLE IF NOT EXISTS lpts_copyright_organizations"
			" (lpts_copyright varchar(256) NOT NULL,"
			" organization_id int(10) unsigned NOT NULL, "
			" FOREIGN KEY (organization_id)"
			" REFERENCES organizations(id))"
			" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
		self.db.execute(sql, ())
		self.db.execute("TRUNCATE TABLE lpts_copyright_organizations", ())


	def processAlansLicensorMatch(self):
		with open("/Volumes/FCBH/database_data/alan_licensors.csv", newline="\n") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				orgId = int(row[0])
				if len(row) > 2:
					licensor = row[1] + "," + row[2]
				else:
					Licensor = row[1]
				self.licensorUpdates.add((licensor, str(orgId)))
		#print(self.licensorUpdates)


	def processAlansCopyrightMatch(self):
		with open("/Volumes/FCBH/database_data/alan_copyright_holders.csv", newline="\n", encoding='utf-16') as csvfile:
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
		sql = "UPDATE organizations SET lpts_licensor = %s WHERE id = %s"
		self.db.executeBatch(sql, list(self.licensorUpdates))
		sql = "INSERT INTO lpts_copyright_organizations (lpts_copyright, organization_id) VALUES (%s, %s)"
		inserts = list(self.copyrightInserts)
		for index in range(len(inserts)):
			item = inserts[index]
			try:
				self.db.execute(sql, item)
			except:
				(copyright, orgId) = item
				print("rejected", index, copyright, orgId)


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	test = MatchOrganizationNames(config, db, lptsReader)
	test.addColumns()
	test.processAlansLicensorMatch()
	test.processAlansCopyrightMatch()
	test.processLicensor()
	test.processCopyHolder()
	test.displayOutput()
	test.updateDatabase()
	db.close()

