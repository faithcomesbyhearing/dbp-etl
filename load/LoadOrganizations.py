# LoadOrganizations.py

import os
import re

class LoadOrganizations:
	def __init__(self, config, db, dbOut, languageReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.languageReader = languageReader
		self.LicensorHosanna = "Hosanna"
		self.organization_map = {}
		# It updates the regex to use the SQL temporary variable as a reference in the creation
		# of the organization and organization translations
		self.dbOut.UpdateUnquotedRegex(re.compile(r"(.*?)(\'@[^']*?\')(.*$)"))

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


	def get_lpts_licensors(self, language_record, fileset_id, type_code):
		licensors = []
		if language_record.Has_Ambiguous_Agreement():
			print("WARN ambiguos agreement and it needs to be fixed for fileset id: %s" % (fileset_id))
			return None

		if language_record.Has_Other_Agreement():
			print("WARN licensor: %s and co licensor: %s cannot be processed due to 'other Agreement' for fileset id: %s" % (language_record.Licensor(), language_record.CoLicensor(), fileset_id))
			return None

		if language_record.Has_Lora_Agreement() and type_code == "audio":
			self.add_licensor_if_not_empty(licensors, language_record.Licensor(), fileset_id, "licensor")
			return licensors

		if language_record.Has_Lora_Agreement() and type_code == "text":
			self.add_licensor_if_not_empty(licensors, language_record.CoLicensor(), fileset_id, "co-licensor")
			if language_record.CoLicensor() == None and language_record.Licensor() != None:
				print("WARN %s has ambiguos text agreement, licensor: %s may or may not be the text licensor" % (fileset_id, language_record.Licensor()))
			return licensors

		if language_record.Has_Text_Agreement() and type_code == "text":
			self.add_licensor_if_not_empty(licensors, language_record.Licensor(), fileset_id, "licensor")
			self.add_licensor_if_not_empty(licensors, language_record.CoLicensor(), fileset_id, "co-licensor")
			return licensors
		
		if language_record.Has_Text_Agreement() and type_code == "audio" and language_record.Reg_Recording_Status() != "Text Only":
			self.add_licensor_if_not_empty(licensors, self.LicensorHosanna, fileset_id, "licensor")
			return licensors
		
		if language_record.Reg_Recording_Status() == "Text Only" and type_code == "text":
			self.add_licensor_if_not_empty(licensors, language_record.Licensor(), fileset_id, "licensor")
			self.add_licensor_if_not_empty(licensors, language_record.CoLicensor(), fileset_id, "co-licensor")
			return licensors

		return None

	def add_licensor_if_not_empty(self, licensors, licensor, fileset_id, role):
		if licensor:
			licensors.append(licensor)
		else:
			print(f"WARN {role} is empty for fileset id: {fileset_id}")

	def process_licensors(self, dbp_org_list, lpts_licensors):
		# Initialize all current DB organizations with the "ToDelete" status
		licensor_in_dbp = {org: "ToDelete" for org in dbp_org_list} if dbp_org_list else {}
		for licensor in lpts_licensors:
			org_id = self.organization_map.get(licensor)
			# If the licensor does not exist it will create a new licensor (organization) record
			if org_id == None:
				org_id = self.create_organization(licensor)
				# update the organization map with the sql temp variable
				self.organization_map[licensor] = org_id

			self.update_licensor_status(licensor_in_dbp, org_id, dbp_org_list)
		return licensor_in_dbp

	def update_licensor_status(self, licensor_in_dbp, org_id, dbp_org_list):
		if org_id:
			licensor_in_dbp[org_id] = "Existing" if org_id in dbp_org_list else "New"

	def process_fileset(self, fileset, dbp_org_map):
		bible_id, fileset_id, type_code_prefix, _, _, hash_id = fileset
		type_code = type_code_prefix.split("_")[0]
		dbp_fileset_id = fileset_id if fileset_id[8:10] != "SA" else fileset_id[:8] + "DA" + fileset_id[10:]

		language_record = self.languageReader.getLanguageRecordLoose(type_code, bible_id, dbp_fileset_id)[0]
		if not language_record:
			return

		lpts_licensors = self.get_lpts_licensors(language_record, fileset_id, type_code)
		if lpts_licensors == None:
			return

		licensor_in_dbp = self.process_licensors(dbp_org_map.get(hash_id), lpts_licensors)
		self.update_database(licensor_in_dbp, hash_id)

	def create_organization(self, licensor_name):
		table_name_org = "organizations"
		table_name_org_trans = "organization_translations"
		licensor_name_slug = licensor_name.replace(" ", "_")
		# Create a unique temp variable to store the organization id to create the relationship
		licensor_name_sql_key = "@" + licensor_name.replace(" ", "_")

		# organization
		org_attr_names = ("slug",)
		org_values = [(licensor_name_slug,)]

		# organization_translations
		org_trans_values = [(licensor_name_sql_key, licensor_name, licensor_name, 6414)]
		org_trans_attr_values = ("organization_id", "name", "description")
		org_trans_pkey_names = ("language_id",)

		self.dbOut.insert(table_name_org, (), org_attr_names, org_values, 0)
		self.dbOut.insert(table_name_org_trans, org_trans_pkey_names, org_trans_attr_values, org_trans_values)

		return licensor_name_sql_key

	def update_database(self, licensor_in_dbp, hash_id):
		inserts, deletes = [], []
		for licensor, status in licensor_in_dbp.items():
			if status == "New":
				inserts.append((licensor, hash_id, 2))
			elif status == "ToDelete":
				deletes.append((hash_id, licensor, 2))

		table_name = "bible_fileset_copyright_organizations"
		attr_names = ("organization_id",)
		pkey_names = ("hash_id", "organization_role")
		pkey_names_to_delete = ("hash_id", "organization_id", "organization_role")

		self.dbOut.insert(table_name, pkey_names, attr_names, inserts)
		self.dbOut.delete(table_name, pkey_names_to_delete, deletes)

	## This method updates the bible_fileset_copyright_organization with licensors
	def update_licensors(self, fileset_list):
		# Initialize database and logging setup
		# Retrive all ltps_organizations records
		self.organization_map = self.db.selectMap("SELECT ot.name, o.id FROM organizations o INNER JOIN organization_translations ot ON o.id = ot.organization_id", ())
		dbp_org_map = self.db.selectMapList("SELECT hash_id, organization_id FROM bible_fileset_copyright_organizations WHERE organization_role=2", ())

		for fileset in fileset_list:
			self.process_fileset(fileset, dbp_org_map)
		

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
	from Config import Config
	from SQLUtility import SQLUtility
	from SQLBatchExec import SQLBatchExec
	from LanguageReaderCreator import LanguageReaderCreator

	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	migration_stage = "B" if os.getenv("DATA_MODEL_MIGRATION_STAGE") == None else os.getenv("DATA_MODEL_MIGRATION_STAGE")
	print("migration stage:", migration_stage)

	if migration_stage != None:
		languageReader = LanguageReaderCreator(migration_stage).create(config.filename_lpts_xml)
		orgs = LoadOrganizations(config, db, dbOut, languageReader)
		sql = ("SELECT b.bible_id, bf.id, bf.set_type_code, bf.set_size_code, bf.asset_id, bf.hash_id"
			" FROM bible_filesets bf JOIN bible_fileset_connections b ON bf.hash_id = b.hash_id"
			" ORDER BY b.bible_id, bf.id, bf.set_type_code"
			" LOCK IN SHARE MODE")
		filesetList = db.select(sql, ())
		if filesetList != None:
			print("num filelists", len(filesetList))
			orgs.update_licensors(filesetList)
			dbOut.displayStatements()
			dbOut.displayCounts()
			dbOut.execute("test-orgs")
			db.close()


