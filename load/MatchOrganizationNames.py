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

		self.licensorsNoMatch = set()
		self.copyHolderNoMatch = set()
		self.organizationMap = {}
		#resultSet = self.db.select("select id, slug from organizations where id in (select organization_id from bible_fileset_copyright_organizations) order by slug", ())
		with open("/Volumes/FCBH/database_data/organizations.csv", newline="\n") as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				self.organizationMap[int(row[0])] = (int(row[0]), row[1], set(), set())
		self.langMap = {}
		#self.langMap = self.db.selectMap("SELECT name, organization_id FROM organization_translations where organization_id in (select organization_id from bible_fileset_copyright_organizations) order by name", ())
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
		copyright = self.reduceName(copyHolder)
		orgId = self.langMap.get(copyright)
		if orgId != None:
			organization = self.organizationMap[orgId]
			(orgId, slug, licensors, copyHolders) = organization
			copyHolders.add(copyright)
			self.organizationMap[orgId] = (orgId, slug, licensors, copyHolders)
		else:
			self.copyHolderNoMatch.add(copyright)


	def reduceName(self, name):
		pos = 0
		if "©" in name:
			pos = name.index("©")
		elif "℗" in name:
			pos = name.index("℗")
		elif "®" in name:
			pos = name.index("®")
		if pos > 0:
			name = name[pos:]
		for char in ["©", "℗", "®","0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ",", "/", "-", ";", "_", ".","\n"]:
			name = name.replace(char, "")
		name = name.replace("     ", " ")
		name = name.replace("    ", " ")
		name = name.replace("   ", " ")
		name = name.replace("  ", " ")
		name = name[:64]
		return name.strip()


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



if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	test = MatchOrganizationNames(config, db, lptsReader)
	test.processLicensor()
	test.processCopyHolder()
	test.displayOutput()
	db.close()

