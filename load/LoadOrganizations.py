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


	## This program requires the primary key of bible_fileset_copyright_organizations
	## to be hash_id, organization_id, organization_role.
	## If it is not, it updates the key.
	## Written in July 2020, this is a temporary method that can be eliminated
	def changeCopyrightOrganizationsPrimaryKey(self):
		sql = (("SELECT column_name FROM information_schema.key_column_usage"
			" WHERE table_name='bible_fileset_copyright_organizations'"
			" AND table_schema='%s'"
			" AND constraint_name='PRIMARY'"
			" ORDER BY ordinal_position") % (self.config.database_db_name))
		pkey = self.db.selectList(sql, ())
		if len(pkey) == 2 and pkey[0] == "hash_id" and pkey[1] == "organization_id":
			sql = (("ALTER TABLE %s.bible_fileset_copyright_organizations"   
  				" DROP PRIMARY KEY,"
  				" ADD PRIMARY KEY (hash_id, organization_id, organization_role)") 
				% (self.config.database_db_name))
			self.db.execute(sql, ())
			print("The primary key of bible_fileset_copyright_organizations was changed.")
			print("The key is now hash_id, organization_id, organization_role.")


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
			dbpOrgList = self.db.selectSet(sql, (hashId))
			(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)
			lptsOrgList = set()
			if lptsRecord != None and self.hasDamIds(lptsRecord, "text"):
				for licensor in [lptsRecord.Licensor(), lptsRecord.CoLicensor()]:
					if licensor != None:
						licensorOrg = organizationMap.get(licensor)
						if licensorOrg != None:
							lptsOrgList.add(licensorOrg)
						else:
							print("ERROR: There is no org_id for: %s" % (licensor))
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
	def updateCopyrightHolders(self, filesetList):
		inserts = []
		deletes = []
		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=1"
		organizationMap = self.db.selectMap(sql, ())
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			sql = "SELECT organization_id FROM bible_fileset_copyright_organizations WHERE hash_id = %s AND organization_role=1"
			dbpOrgList = self.db.selectSet(sql, (hashId))
			(lptsRecord, lptsIndex) = self.lptsReader.getLPTSRecordLoose(typeCode, bibleId, filesetId)
			lptsList = set()
			lptsOrgList = set()
			if lptsRecord != None:
				if self.hasDamIds(lptsRecord, "text") and lptsRecord.Copyrightc() != None:
					lptsList.add(lptsRecord.Copyrightc())
				if self.hasDamIds(lptsRecord, "audio") and lptsRecord.Copyrightp() != None:
					lptsList.add(lptsRecord.Copyrightp())
				if self.hasDamIds(lptsRecord, "video") and lptsRecord.Copyright_Video() != None:
					lptsList.add(lptsRecord.Copyright_Video())

				for copyright in lptsList:
					name = self.lptsReader.reduceCopyrightToName(copyright)
					copyrightOrg = organizationMap.get(name)
					if copyrightOrg != None:
						lptsOrgList.add(copyrightOrg)
					else:
						print("ERROR: There is no org_id for: %s" % (name))
			for lptsOrg in lptsOrgList:
				if lptsOrg not in dbpOrgList:
					inserts.append((hashId, lptsOrg, 1))
			for dbpOrg in dbpOrgList:
				if dbpOrg not in lptsOrgList:
					deletes.append((hashId, dbpOrg))

		tableName = "bible_fileset_copyright_organizations"
		pkeyNames = ("hash_id", "organization_id")
		attrNames = ("organization_role",)
		dbOut.insert(tableName, pkeyNames, attrNames, inserts)
		dbOut.delete(tableName, pkeyNames, deletes)


	## After licensors have been updated in bible_fileset_copyright_organizations,
	## this method can be run to verify the correctness of that update.
	## This method should be kept for use anytime the updateLicensors is modified.
	def unitTestUpdateLicensors(self):
		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=2"
		organizationMap = self.db.selectMap(sql, ())
		totalDamIdSet = set()
		for lptsRecord in self.lptsReader.resultSet:
			licensorSet = set()
			for licensor in [lptsRecord.Licensor(), lptsRecord.CoLicensor()]:
				if licensor != None:
					licensorOrg = organizationMap.get(licensor)
					if licensorOrg != None:
						licensorSet.add(licensorOrg)
			textDam1 = lptsRecord.DamIds("text", 1)
			textDam2 = lptsRecord.DamIds("text", 2)
			textDam3 = lptsRecord.DamIds("text", 3)
			textDamIds = textDam1.union(textDam2).union(textDam3)
			for damId in textDamIds:
				totalDamIdSet.add(damId)
				sql = ("SELECT hash_id FROM bible_filesets WHERE id=%s"
					" AND set_type_code IN ('text_format', 'text_plain')")
				hashId = self.db.selectScalar(sql, (damId))
				if hashId != None:
					sql = ("SELECT organization_id FROM bible_fileset_copyright_organizations"
						" WHERE hash_id=%s AND organization_role = 2")
					orgSet = self.db.selectSet(sql, (hashId))
					if orgSet != licensorSet:
						print("ERROR:", hashId, damId, " LPTS:", licensorSet, " DBP:", orgSet)
		# Check the database for damId's not processed by this check


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	orgs = LoadOrganizations(config, db, dbOut, lptsReader)
	orgs.changeCopyrightOrganizationsPrimaryKey()
	#unknownLicensors = orgs.validateLicensors()
	#unknownCopyrights = orgs.validateCopyrights()
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code")
	filesetList = db.select(sql, ())
	print("num filelists", len(filesetList))
	#orgs.updateLicensors(filesetList)
	#orgs.updateCopyrightHolders(filesetList)
	#dbOut.displayStatements()
	#dbOut.displayCounts()
	#dbOut.execute()

	orgs.unitTestUpdateLicensors()
	db.close()


"""
SET FOREIGN_KEY_CHECKS=0;
insert into bible_fileset_copyright_organizations 
select * from bible_fileset_copyright_organizations_backup;
SET FOREIGN_KEY_CHECKS=1;
"""
