# LoadOrganizations.py

import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *
from SQLBatchExec import *

class LoadOrganizations:


	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
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
	def updateLicensors(self, filesetList):
		inserts = []
		deletes = []
		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=2"
		organizationMap = self.db.selectMap(sql, ())
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			sql = "SELECT organization_id FROM bible_fileset_copyright_organizations WHERE hash_id = %s AND organization_role=2"
			dbpOrgList = self.db.selectList(sql, (hashId))
			(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)
			lptsOrgList = []
			if lptsRecord != None and self.hasDamIds(lptsRecord, "text"):
				if lptsRecord.Licensor() != None:
					licensorOrg = organizationMap.get(lptsRecord.Licensor())
					if licensorOrg != None:
						lptsOrgList.append(licensorOrg)
					else:
						print("ERROR: There is no org_id for: %s" % (lptsRecord.Licensor()))
				if lptsRecord.CoLicensor() != None:
					coLicensorOrg = organizationMap.get(lptsRecord.CoLicensor())
					if coLicensorOrg != None:
						lptsOrgList.append(coLicensorOrg)
					else:
						print("ERROR: There is no org_id for: %s" % (lptsRecord.CoLicensor()))

			for lptsOrg in lptsOrgList:
				if lptsOrg not in dbpOrgList:
					inserts.append((hashId, lptsOrg, 2))

			for dbpOrg in dbpOrgList:
				if dbpOrg not in lptsOrgList:
					deletes.append((hashId, dbpOrg))

		tableName = "bible_fileset_copyright_organizations"
		pkeyNames = ("hash_id", "organization_id")
		attrNames = ("organization_role",)
		dbOut.insert(tableName, pkeyNames, attrNames, inserts)
		dbOut.delete(tableName, pkeyNames, deletes)
		

	## This method updates the bible_fileset_copyright_organizations with copyrightholders
	def updateCopyrightHolders(self):
		print("tbd")


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	orgs = LoadOrganizations(config, db, dbOut, lptsReader)
	unknownLicensors = orgs.validateLicensors()
	#unknownCopyrights = orgs.validateCopyrights()
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
	filesetList = db.select(sql, ())
	print("num filelists", len(filesetList))
	licensorUpdates = orgs.updateLicensors(filesetList)


	db.close()
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute()
