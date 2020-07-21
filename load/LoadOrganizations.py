# LoadOrganizations.py

import io
import os
import sys
#import csv
#import operator
#from datetime import datetime
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class LoadOrganizations:


	def __init__(self, config, db, lptsReader):
		self.config = config
		self.db = db
		self.lptsReader = lptsReader


	## This method looks for licensors that are in LPTS, 
	## but not yet known to dbp
	def validateLicensors(self):
		resultSet = set()
		sql = "SELECT lpts_organization FROM lpts_organizations WHERE organization_role=2"
		licensorSet = self.db.selectSet(sql, ())
		for record in self.lptsReader.resultSet:
			if self.hasDamIds(record, "text"):
				licensor = record.Licensor()
				coLicensor = record.CoLicensor()
				if licensor != None and licensor not in licensorSet:
					resultSet.add(licensor)
				if coLicensor != None and coLicensor not in licensorSet:
					resultSet.add(coLicensor)
		for item in resultSet:
			print("Licensor missing from lpts_organizations:", item)
		return resultSet


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


	## This method looks for copyright holders that are in LPTS,
	## but not yet known to dbp
	def validateCopyrights(self):
		resultSet = set()
		sql = "SELECT lpts_organization FROM lpts_organizations WHERE organization_role=1"
		copyrightSet = self.db.selectSet(sql, ())
		for record in self.lptsReader.resultSet:
			if self.hasDamIds(record, "text"):
				self.copyrightMatch(resultSet, record.Copyrightc(), copyrightSet)
			if self.hasDamIds(record, "audio"):
				self.copyrightMatch(resultSet, record.Copyrightp(), copyrightSet)
			if self.hasDamIds(record, "video"):
				self.copyrightMatch(resultSet, record.Copyright_Video(), copyrightSet)
		for item in resultSet:
			print("Copyright missing from lpts_organizations:", item)
		return resultSet


	def copyrightMatch(self, resultSet, copyright, copyrightSet):
		if copyright != None:
			name = self.lptsReader.reduceCopyrightToName(copyright)
			if not name in copyrightSet:
				resultSet.add(name)


	## This method updates the bible_fileset_copyright_organization with licensors
	def updateLicensors(self):
		print("tbd")


	## This method updates the bible_fileset_copyright_organizations with copyrightholders
	def updateCopyrightHolders(self):
		print("tbd")



if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	db = SQLUtility(config)
	orgs = LoadOrganizations(config, db, lptsReader)
	#unknownLicensors = orgs.validateLicensors()
	unknownCopyrights = orgs.validateCopyrights()


	db.close()	