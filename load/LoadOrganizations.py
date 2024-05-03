# LoadOrganizations.py

import io
import os
import sys
from Config import *
from LanguageReader import *
from SQLUtility import *
from SQLBatchExec import *

class LoadOrganizations:


	def __init__(self, config, db, dbOut, languageReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.languageReader = languageReader


	## This program requires the primary key of bible_fileset_copyright_organizations
	## to be hash_id, organization_id, organization_role.
	## If it is not, it updates the key.
	## Written in July 2020, this is a temporary method that can be eliminated
#	def changeCopyrightOrganizationsPrimaryKey(self):
#		sql = (("SELECT column_name FROM information_schema.key_column_usage"
#			" WHERE table_name='bible_fileset_copyright_organizations'"
#			" AND table_schema='%s'"
#			" AND constraint_name='PRIMARY'"
#			" ORDER BY ordinal_position") % (self.config.database_db_name))
#		pkey = self.db.selectList(sql, ())
#		if len(pkey) == 2 and pkey[0] == "hash_id" and pkey[1] == "organization_id" or len(pkey) == 3:
#			sql = "TRUNCATE TABLE %s.bible_fileset_copyright_organizations" % (self.config.database_db_name)
#			self.db.execute(sql, ())
#			sql = (("ALTER TABLE %s.bible_fileset_copyright_organizations"   
#  				" DROP PRIMARY KEY,"
#  				" ADD PRIMARY KEY (hash_id, organization_role)") 
#				% (self.config.database_db_name))
#			self.db.execute(sql, ())
#			print("The primary key of bible_fileset_copyright_organizations was changed.")
#			print("The key is now hash_id, organization_role.")


	## This method looks for licensors that are in LPTS, 
	## but not yet known to dbp
#	def validateLicensors(self):
#		resultSet = set()
#		sql = "SELECT lpts_organization FROM lpts_organizations WHERE organization_role=2"
#		licensorSet = self.db.selectSet(sql, ())
#		for record in self.languageReader.resultSet:
#			if self.hasDamIds(record, "text"):
#				licensor = record.Licensor()
#				coLicensor = record.CoLicensor()
#				if licensor != None and licensor not in licensorSet:
#					resultSet.add(licensor)
#				if coLicensor != None and coLicensor not in licensorSet:
#					resultSet.add(coLicensor)
#		for item in resultSet:
#			print("Licensor missing from lpts_organizations:", item)
#		return resultSet


	def hasDamIds(self, languageRecord, typeCode):
		damIds = languageRecord.DamIdMap(typeCode, 1)
		if len(damIds) > 0:
			return True
		damIds = languageRecord.DamIdMap(typeCode, 2)
		if len(damIds) > 0:
			return True
		damIds = languageRecord.DamIdMap(typeCode, 2)
		if len(damIds) > 0:
			return True
		return False


	## This method looks for copyright holders that are in LPTS,
	## but not yet known to dbp
#	def validateCopyrights(self):
#		resultSet = set()
#		sql = "SELECT lpts_organization FROM lpts_organizations WHERE organization_role=1"
#		copyrightSet = self.db.selectSet(sql, ())
#		for record in self.languageReader.resultSet:
#			if self.hasDamIds(record, "text"):
#				self.copyrightMatch(resultSet, record.Copyrightc(), copyrightSet)
#			if self.hasDamIds(record, "audio"):
#				self.copyrightMatch(resultSet, record.Copyrightp(), copyrightSet)
#			if self.hasDamIds(record, "video"):
#				self.copyrightMatch(resultSet, record.Copyright_Video(), copyrightSet)
#		for item in resultSet:
#			print("Copyright missing from lpts_organizations:", item)
#		return resultSet


#	def copyrightMatch(self, resultSet, copyright, copyrightSet):
#		if copyright != None:
#			name = self.languageReader.reduceCopyrightToName(copyright)
#			if not name in copyrightSet:
#				resultSet.add(name)


	## This method updates the bible_fileset_copyright_organization with licensors
	def updateLicensors(self, filesetList):
		inserts = []
		updates = []
		deletes = []
		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=2"
		organizationMap = self.db.selectMap(sql, ())
		sql = "SELECT hash_id, organization_id FROM bible_fileset_copyright_organizations WHERE organization_role=2"
		dbpOrgMap = self.db.selectMap(sql, ())
		for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
			typeCode = setTypeCode.split("_")[0]
			if filesetId[8:10] == "SA":
				dbpFilesetId = filesetId[:8] + "DA" + filesetId[10:]
			else:
				dbpFilesetId = filesetId
			(languageRecord, lptsIndex) = self.languageReader.getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId)
			licensorOrg = None
			licensorOrg2 = None
			if languageRecord != None:

# BWF notes May 3, 2024 (remove before submitting PR)
#
# role is no longer relevant, but we'll leave it for now, using only role=2 and never role = 1
#
# if more than one agreement Type is selected (could be LORA, TextAgreement, Other). 
# 			log statement that this data needs to be fixed
#           continue
#typeCode = setTypeCode.split("_")[0]
# if languageReader.LORA = 1
	# if Typecode = Audio
#    	lptsLicensor = languageRecord.Licensor()
#		if languageRecord.CoLicensor() is empty
#			log a statement - languageRecord.Licensor may or may not be text licensor - needs investigation
	# else	if Typecode = Text and languageRecord.CoLicensor is not empty
# 		lptsLicensor = languageRecord.CoLicensor


# else if languageReader.TextAgreement = 1 (ticket 2173)
#	if Type = Text
#    	lptsLicensor = languageRecord.Licensor 
#    	lptsLicensor2 = languageRecord.CoLicensor if CoLicensor is not empty
# 	else if Type = Audio
#    	set licensor = Hosanna	(create a constant called "Hosanna")
			# old -- remove			
				# lptsLicensor = languageRecord.Licensor()
				# if lptsLicensor == None:
				# 	lptsLicensor = languageRecord.CoLicensor()
				# if lptsLicensor == None:
				# 	print("WARN %s has no licensor field." % (filesetId))
				# else:
					#name = self.languageReader.reduceCopyrightToName(lptsLicensor)
	 

	 	if lptsLicensor != None
					licensorOrg = organizationMap.get(lptsLicensor)
					if licensorOrg == None:
						print("WARN %s has no org_id for licensor: %s" % (filesetId, lptsLicensor))
	 	# needed for TextAgreement (ticket 2173)
		# if lptsLicensor2 != None
		# 			licensorOrg2 = organizationMap.get(lptsLicensor2)
		# 			if licensorOrg2 == None:
		# 				print("WARN %s has no org_id for licensor: %s" % (filesetId, lptsLicensor2))
		
		# BWF: now, we may have two licensor orgs for the same hashId, so retrieve a collection instead of just one
		# this only applies to Text Agreement (2173)
		dbpOrgList = dbpOrgMap.get(hashId)
		
		# maybe make a licensorOrgList
		# logic to match licensorOrgList entries to dbpOrgList entries
		# 
		if licensorOrg != None and dbpOrg == None:
			inserts.append((licensorOrg, hashId, 2))
		elif licensorOrg == None and dbpOrg != None:
			deletes.append((hashId, 2))
		elif licensorOrg != dbpOrg:
			updates.append(("organization_id", licensorOrg, dbpOrg, hashId, 2))

	
   
   

		tableName = "bible_fileset_copyright_organizations"
		pkeyNames = ("hash_id", "organization_role")
		attrNames = ("organization_id",)
		self.dbOut.insert(tableName, pkeyNames, attrNames, inserts)
		self.dbOut.updateCol(tableName, pkeyNames, updates)
		self.dbOut.delete(tableName, pkeyNames, deletes)
		

# BWF 5/3/24 - distinguishing between licensor and holder is not relevant. remove this method.
	# ## This method updates the bible_fileset_copyright_organizations with copyrightholders
	# def updateCopyrightHolders(self, filesetList):
	# 	inserts = []
	# 	updates = []
	# 	deletes = []
	# 	sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=1"
	# 	organizationMap = self.db.selectMap(sql, ())
	# 	sql = "SELECT hash_id, organization_id FROM bible_fileset_copyright_organizations WHERE organization_role=1"
	# 	dbpOrgMap = self.db.selectMap(sql, ())
	# 	for (bibleId, filesetId, setTypeCode, setSizeCode, assetId, hashId) in filesetList:
	# 		typeCode = setTypeCode.split("_")[0]
	# 		if filesetId[8:10] == "SA":
	# 			dbpFilesetId = filesetId[:8] + "DA" + filesetId[10:]
	# 		else:
	# 			dbpFilesetId = filesetId
	# 		copyrightOrg = None
	# 		if typeCode != "app":
	# 			(languageRecord, lptsIndex) = self.languageReader.getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId)
	# 			if languageRecord != None:
	# 				lptsCopyright = None

	# 				if typeCode == "text":
	# 					if self.hasDamIds(languageRecord, "text") and languageRecord.Copyrightc() != None:
	# 						lptsCopyright = languageRecord.Copyrightc()
	# 				elif typeCode == "audio":
	# 					if self.hasDamIds(languageRecord, "audio") and languageRecord.Copyrightp() != None:
	# 						lptsCopyright = languageRecord.Copyrightp()
	# 				elif typeCode == "video":
	# 					if self.hasDamIds(languageRecord, "video") and languageRecord.Copyright_Video() != None:
	# 						lptsCopyright = languageRecord.Copyright_Video()

	# 				if lptsCopyright != None:
	# 					name = self.languageReader.reduceCopyrightToName(lptsCopyright)
	# 					copyrightOrg = organizationMap.get(name)
	# 					if copyrightOrg == None:
	# 						print("WARN %s has no org_id for copyright: %s" % (filesetId, name))

	# 		dbpOrg = dbpOrgMap.get(hashId)
	# 		if copyrightOrg != None and dbpOrg == None:
	# 			inserts.append((copyrightOrg, hashId, 1))
	# 		elif copyrightOrg == None and dbpOrg != None:
	# 			deletes.append((hashId, 1))
	# 		elif copyrightOrg != dbpOrg:
	# 			updates.append(("organization_id", copyrightOrg, dbpOrg, hashId, 1))

	# 	tableName = "bible_fileset_copyright_organizations"
	# 	pkeyNames = ("hash_id", "organization_role")
	# 	attrNames = ("organization_id",)
	# 	self.dbOut.insert(tableName, pkeyNames, attrNames, inserts)
	# 	self.dbOut.updateCol(tableName, pkeyNames, updates)
	# 	self.dbOut.delete(tableName, pkeyNames, deletes)


	## After licensors have been updated in bible_fileset_copyright_organizations,
	## this method can be run to verify the correctness of that update.
	## This method should be kept for use anytime the updateLicensors is modified.
#	def unitTestUpdateLicensors(self):
#		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=2"
#		organizationMap = self.db.selectMapSet(sql, ())
#		totalDamIdSet = set()
#		for languageRecord in self.languageReader.resultSet:
#			licensorSet = set()
#			for licensor in [languageRecord.Licensor(), languageRecord.CoLicensor()]:
#				if licensor != None:
#					licensorOrg = organizationMap.get(licensor)
#					if licensorOrg != None:
#						licensorSet = licensorSet.union(licensorOrg)
#			textDam1 = set(languageRecord.DamIdMap("text", 1).keys())
#			textDam2 = set(languageRecord.DamIdMap("text", 2).keys())
#			textDam3 = set(languageRecord.DamIdMap("text", 3).keys())	
#			textDamIds = textDam1.union(textDam2).union(textDam3)
#			dbpOrgSet = set()
#			for damId in textDamIds:
#				totalDamIdSet.add(damId)
#				sql = ("SELECT hash_id FROM bible_filesets WHERE id=%s"
#					" AND set_type_code IN ('text_format', 'text_plain')")
#				hashIds = self.db.selectList(sql, (damId))
#				for hashId in hashIds:
#					sql = ("SELECT organization_id FROM bible_fileset_copyright_organizations"
#						" WHERE hash_id=%s AND organization_role = 2")
#					orgSet = self.db.selectSet(sql, (hashId))
#					dbpOrgSet = dbpOrgSet.union(orgSet)
#				if hashIds != [] and dbpOrgSet != licensorSet:
#					print("ERROR:", hashId, damId, " LPTS:", licensorSet, " DBP:", dbpOrgSet)
#
#		self.db.execute("CREATE TEMPORARY TABLE damid_check (damid varchar(20))", ())
#		self.db.executeBatch("INSERT INTO damid_check (damid) VALUES (%s)", list(totalDamIdSet))
#		count = self.db.selectScalar("SELECT count(*) FROM damid_check", ())
#		print(count, "records inserted")
#		sql = ("SELECT id, hash_id FROM bible_filesets"
#			" WHERE set_type_code IN ('text_plain', 'text_format')"
#			" AND id NOT IN (SELECT damid FROM damid_check)")
#		missedList = self.db.select(sql, ())
#		for (filesetId, hashId) in missedList:
#			print("MISSED:", filesetId, hashId)


	## After copyrightholders have been updated in bible_fileset_copyright_organizations,
	## this method can be run to verify the correctness of that update.
	## This method should be kept for use anytime the updateCopyrightHolders is modified.
#	def unitTestUpdateCopyrightHolders(self):
#		sql = "SELECT lpts_organization, organization_id FROM lpts_organizations WHERE organization_role=1"
#		organizationMap = self.db.selectMapSet(sql, ())
#		totalDamIdSet = set()
#		for languageRecord in self.languageReader.resultSet:
#			self.compareOneTypeCode(totalDamIdSet, organizationMap, languageRecord, "text")
#			self.compareOneTypeCode(totalDamIdSet, organizationMap, languageRecord, "audio")
#			self.compareOneTypeCode(totalDamIdSet, organizationMap, languageRecord, "video")
#
#		self.db.execute("CREATE TEMPORARY TABLE damid_check2 (damid varchar(20))", ())
#		self.db.executeBatch("INSERT INTO damid_check2 (damid) VALUES (%s)", list(totalDamIdSet))
#		count = self.db.selectScalar("SELECT count(*) FROM damid_check2", ())
#		print(count, "records inserted")
#		sql = ("SELECT id, hash_id FROM bible_filesets"
#			" WHERE RIGHT(id, 2) != '16'"
#			" AND id NOT IN (SELECT damid FROM damid_check2)")
#		missedList = self.db.select(sql, ())
#		for (filesetId, hashId) in missedList:
#			print("MISSED:", filesetId, hashId)


#	def compareOneTypeCode(self, totalDamIdSet, organizationMap, languageRecord, typeCode):
#		copyright = None
#		copyrightName = None
#		copyrightOrgSet = set()
#
#		if typeCode == "text":
#			copyright = languageRecord.Copyrightc()
#		elif typeCode == "audio":
#			copyright = languageRecord.Copyrightp()
#		elif typeCode == "video":
#			copyright = languageRecord.Copyright_Video()
#		if copyright != None:
#			copyrightName = self.languageReader.reduceCopyrightToName(copyright)
#			copyrightOrgSet = organizationMap.get(copyrightName, set())
#
#		if typeCode != "video":
#			dam1Set = set(languageRecord.DamIdMap(typeCode, 1).keys())
#			dam2Set = set(languageRecord.DamIdMap(typeCode, 2).keys())
#			dam3Set = set(languageRecord.DamIdMap(typeCode, 3).keys())	
#			lptsDamIds = dam1Set.union(dam2Set).union(dam3Set)		
#		else:
#			lptsDamIds = set(languageRecord.DamIdMap("video", 1).keys())
#
#		dbpOrgSet = set()
#		for damId in lptsDamIds:
#			totalDamIdSet.add(damId)
#			sql = ("SELECT hash_id FROM bible_filesets WHERE id='%s' AND LEFT(set_type_code,4) = '%s'")
#			stmt = sql % (damId, typeCode[:4])
#			hashIds = self.db.selectList(stmt, ())
#			for hashId in hashIds:
#				sql = ("SELECT organization_id FROM bible_fileset_copyright_organizations"
#					" WHERE hash_id=%s AND organization_role = 1")
#				orgSet = self.db.selectSet(sql, (hashId))
#				dbpOrgSet = dbpOrgSet.union(orgSet)
#			if hashIds != [] and dbpOrgSet != copyrightOrgSet:
#				print("ERROR:", hashId, damId, copyrightName, " LPTS:", copyrightOrgSet, " DBP:", dbpOrgSet)


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	orgs = LoadOrganizations(config, db, dbOut, languageReader)
	orgs.changeCopyrightOrganizationsPrimaryKey()
	sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
		" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
		" ORDER BY b.bible_id, bf.id, bf.set_type_code"
		" LOCK IN SHARE MODE")
	filesetList = db.select(sql, ())
	print("num filelists", len(filesetList))
	orgs.updateLicensors(filesetList)
	orgs.updateCopyrightHolders(filesetList)
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute("test-orgs")
	db.close()


