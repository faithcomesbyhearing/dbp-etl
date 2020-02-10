# Organizations.py

# This program brings to one place copyright organization data from DBP
# And the LPTS fields used to populate that data.  The purpose of this
# program was to reverse engineer how organization data is loaded into DBP

import io
import os
import sys
import re
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

DROP_CHARS = r"[.,-]"

class Organizations:

	def __init__(self, config):
		self.db = SQLUtility(config)
		self.db.execute("use dbp", ())
		count = self.db.selectScalar("select count(*) from bibles", ())
		print("count", count)


	def process(self, lptsReader):
		orgIdMap = {}
		## This logic assumes there are no names with multiple org ids
		resultSet = self.db.select("SELECT name, organization_id FROM organization_translations", ())
		for row in resultSet:
			#name = re.sub(DROP_CHARS, '', row[0])
			#orgIdMap[name] = row[1]
			orgIdMap[row[0]] = row[1]
		
		self.possibleMapList = self.db.selectMapList("SELECT organization_id, name FROM organization_translations", ())
		sql = ("SELECT bf.id AS fileset_id, bf.hash_id, bf.set_type_code, bfco.organization_role,"
			" o.slug, o.id AS organization_id, bfc.bible_id"
			" FROM bible_filesets bf"
			" JOIN bible_fileset_copyright_organizations bfco ON bfco.hash_id = bf.hash_id"
			" JOIN organizations o ON bfco.organization_id = o.id"
			" JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id"
			" ORDER BY bf.id, bf.hash_id")
		resultSet = self.db.select(sql, ())
		matchCount = 0
		unMatchCount = 0
		skipCount = 0
		for row in resultSet:
			filesetId = row[0]
			hashId = row[1]
			setTypeCode = row[2]
			typeCode = setTypeCode.split("_")[0]
			organizationRole = row[3]
			slug = row[4]
			organizationId = row[5]
			#name = row[6]
			bibleId = row[6]
			if typeCode in {"audio", "text", "video"}:
				#print(filesetId, hashId, typeCode, organizationRole, slug, organizationId, bibleId)
				(record, index, status) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				if record != None:
					textCopyright = record.Copyrightc()
					audioCopyright = record.Copyrightp()
					videoCopyright = record.Copyright_Video()
					licensor = record.Licensor()
					coLicensor = record.CoLicensor()
					creativeCommons = record.CreativeCommonsText()
					electronicPub = record.ElectronicPublisher(index)

					if typeCode == "audio" and organizationRole == 1:
						orgName = self.parseCopyright(record.Copyrightp())
					elif typeCode == "audio" and organizationRole == 2:
						orgName = self.parseCopyright(record.Licensor())
					elif typeCode == "audio" and organizationRole == 3:
						orgName = "I don't know"
					elif typeCode == "text" and organizationRole == 1:
						orgName = self.parseCopyright(record.Copyrightc())
					elif typeCode == "text" and organizationRole == 2:
						orgName = self.parseCopyright(record.Licensor())
					elif typeCode == "text" and organizationRole == 3:
						orgName = "I don't know"
					elif typeCode == "video" and organizationRole == 1:
						orgName = self.parseCopyright(record.Copyright_Video())
					elif typeCode == "video" and organizationRole == 2:
						orgName = self.parseCopyright(record.licensor())
					elif typeCode == "video" and organizationRole == 3:
						orgName = "I don't know"
					else:
						print("ERROR: unexpected typeCode", typeCode)
						sys.exit()


					#orgId = orgIdMap.get(orgName)
					orgId = self.matchName(orgIdMap, orgName)
					if orgId != None and orgId == organizationId:
						matchCount += 1
						#print("\tmatched:", organizationId, orgName)
					else:
						unMatchCount += 1
						#self.displayNames(bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
						#	textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
						#	creativeCommons, electronicPub, orgName)
						print("DBP: %s/%s/%s hash:%s, role: %s, orgId: %s, orgSlug: %s" %(typeCode, bibleId, filesetId, hashId, organizationRole, organizationId, slug))
						if orgId != None:
							print("\tOrg Lookup Result: %d found for %s" % (orgId, orgName))
						else:
							print("\tOrg Lookup Result: No orgId for %s" % (orgName))
						if textCopyright != None:
							print("\tLPTS Copyrightc:", textCopyright)
						if audioCopyright != None:
							print("\tLPTS Copyrightp:", audioCopyright)
						if videoCopyright != None:
							print("\tLPTS Copyright_Video:", videoCopyright)
						if licensor != None:
							print("\tLPTS Licensor:", licensor)
						if coLicensor != None:
							print("\tLPTS CoLicensor:", coLicensor)
						if creativeCommons != None:
							print("\tLPTS CreativeCommonsText:", creativeCommons)
						if electronicPub != None:
							print("\tLPTS ElectronicPublisher:", electronicPub)
						for possibleName in self.possibleMapList[organizationId]:
							print("\torganization_translations.name for %s: %s" % (organizationId, possibleName))		

				else:
					#print("\t*** NO LPTS")
					skipCount += 1
			else:
				#print("\t SKIP typecode", typeCode)
				skipCount += 1

		print("total=", len(resultSet), " matched=", matchCount, " unmatched=", unMatchCount, " skiped=", skipCount)

	#### this is the way that it is done in lptsmanager
	###orgName in tempDic['Copyrightc']:

	def parseCopyright(self, copyright):
		return copyright
		"""
		result = None
		if copyright != None:
			match = re.search(r"[A-Za-z]", copyright)
			if match != None:
				result = copyright[match.start():]
				result = re.sub(DROP_CHARS, "", result)
			print("*** %s -> |%s|" % (copyright, result))
		return result
		"""

	def matchName(self, orgIdMap, orgName):
		if orgName != None:
			if orgName == "SIL":
				return 30
			#elif orgName == "Aramaic Bible Translation, N.F.P.":
			#	return 588
			#elif orgName == "Davar Partners International":
			#	return 7
			for name in orgIdMap.keys():
				if name in orgName:
					orgId = orgIdMap[name]
					#if orgId==81:
					#	return 848
					if orgId==144:
						return 745
					#elif orgId==48:
					#	return 856
					elif orgId==72:
						return 727
					elif orgId==127:
						return 750
					#elif orgId==132:
					#	return 622
					#elif orgId==254:
					#	return 255
					#elif orgId==915:
					#	return 847

					#elif orgId==440:
					#	return 7
					#elif orgId==81:
					#	return 848
					elif orgId==527:
						return 20
					#elif orgId==703:
					#	return 20
					#elif orgId==254:
					#	return 255
					else:
						return orgId
					#return orgId
		return None

	def displayNames(self, bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
		textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
		creativeCommons, electronicPub, parsedOrgName):
		print("DBP: %s/%s/%s hash:%s, role: %s, orgId: %s, orgSlug: %s" %(typeCode, bibleId, filesetId, hashId, organizationRole, organizationId, slug))
		if textCopyright != None:
			print("\tLPTS Copyrightc:", textCopyright)
		if audioCopyright != None:
			print("\tLPTS Copyrightp:", audioCopyright)
		if videoCopyright != None:
			print("\tLPTS Copyright_Video:", videoCopyright)
		if licensor != None:
			print("\tLPTS Licensor:", licensor)
		if coLicensor != None:
			print("\tLPTS CoLicensor:", coLicensor)
		if creativeCommons != None:
			print("\tLPTS CreativeCommonsText:", creativeCommons)
		if electronicPub != None:
			print("\tLPTS ElectronicPublisher:", electronicPub)
		for possibleName in self.possibleMapList[organizationId]:
			print("\torganization_translations.name for %s: %s" % (organizationId, possibleName))		


"""


SELECT bf.id, bf.hash_id, bf.set_type_code, bfco.organization_role, o.slug, o.id, ot.name
FROM bible_filesets bf
JOIN bible_fileset_copyright_organizations bfco ON bfco.hash_id = bf.hash_id
JOIN organizations o ON bfco.organization_id = o.id
JOIN organization_translations ot ON o.id = ot.organization_id 
ORDER BY bf.id, bf.set_type_code


What I need as a solution to organizations

I need to be able to see side by side
filesetId
each of up to 3 oranizations.slug, organization_role
organizations_translation name
LPTS Copyrightc, Copyrightp, VideoCopyright,
Licensor, CoLicensor, CreativeCommonsText,
ElectronicPublisher1, ElectronicPublisher2, ElectronicPublisher3
"""


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	organizations = Organizations(config)
	organizations.process(lptsReader)

