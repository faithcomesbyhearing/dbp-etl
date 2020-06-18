# MatchOrganizationNames.py
#
# This is a program used in developement, not production
# It matches existing organization names to licensors to produce a list of those
# that match and those that do not.

import io
import os
import sys
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
		resultSet = self.db.select("SELECT id, slug FROM organizations", ())
		for row in resultSet:
			self.organizationMap[row[0]] = (row[0], row[1], set(), set())
		self.langMap = self.db.selectMap("SELECT name, organization_id FROM organization_translations", ())


	def processLicensor(self):
		for record in self.lptsReader.resultSet:
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
			if record.Copyrightp() != None:
				self.matchCopyHolder(record.Copyrightp())
			if record.Copyrightc() != None:
				self.matchCopyHolder(record.Copyrightc())
			if record.Copyright_Video() != None:
				self.matchCopyHolder(record.Copyright_Video())

		print(len(self.copyHolderNoMatch), "Copyright Holdersthat do not match")
		count = 0
		for (orgId, slug, licensors, copyHolders) in self.organizationMap.values():
			if len(copyHolders) > 0:
				count += 1
			if len(copyHolders) > 1:
				print("Duplicate CopyHolder", orgId, slug, copyHolders)
		print(count, "Copyright Holders that do match")	



	def matchCopyHolder(self, copyHolder):
		copyright = self.reduceName(copyHolder)
		orgId = self.langMap.get(copyHolder)
		if orgId != None:
			organization = self.organizationMap[orgId]
			(orgId, slug, licensors, copyHolders) = organization
			copyHolders.add(copyHolder)
			self.organizationMap[orgId] = (orgId, slug, licensors, copyHolders)
		else:
			self.copyHolderNoMatch.add(copyHolder)


	def reduceName(self, name):
		return name


















if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	test = MatchOrganizationNames(config, db, lptsReader)
	test.processLicensor()
	test.processCopyHolder()
	db.close()