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

class Organizations:

	def __init__(self, config):
		self.db = SQLUtility(config)
		self.db.execute("use dbp", ())
		count = self.db.selectScalar("select count(*) from bibles", ())
		print("count", count)


	def process(self, lptsReader):
		orgIdMapList = self.db.selectMapList("SELECT name, organization_id FROM organization_translations", ())
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
					#if textCopyright != None:
					#	print("\ttextCopyright", textCopyright)
					#if audioCopyright != None:
					#	print("\taudioCopyright", audioCopyright)
					#if videoCopyright != None:
					#	print("\tvideoCopyright", videoCopyright)
					#if licensor != None:
					#	print("\tlicensor", licensor)
					#if coLicensor != None:
					#	print("\tcoLicensor", coLicensor)
					#if creativeCommons != None:
					#	print("\tcreativeCommons", creativeCommons)
					#if electronicPub != None:
					#	print("\telectronicPub", electronicPub)

					if typeCode == "audio" and organizationRole == 1:
						copyright = self.parseCopyright(audioCopyright)
						#print("\tParsed Copyright:", copyright)
						orgIds = orgIdMapList.get(copyright)
						if orgIds != None and len(orgIds) == 1 and orgIds[0] == organizationId:
							matchCount += 1
							#print("\tmatched:", organizationId, copyright)
						else:
							unMatchCount += 1
							self.displayNames(bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
								textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
								creativeCommons, electronicPub, copyright)
							#if orgIds != None and len(orgIds) > 0:
							#	for orgId in orgIds:
							#		print("\t*** Found ID", orgId)

					elif typeCode == "text" and organizationRole == 1:
						copyright = self.parseCopyright(textCopyright)
						#print("\tParsed Copyright:", copyright)
						orgIds = orgIdMapList.get(copyright)
						if orgIds != None and len(orgIds) == 1 and orgIds[0] == organizationId:
							matchCount += 1
							#print("\tmatched:", organizationId, copyright)
						else:
							unMatchCount += 1
							self.displayNames(bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
								textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
								creativeCommons, electronicPub, copyright)
							#if orgIds != None and len(orgIds) > 0:
							#	for orgId in orgIds:
							#		print("\t*** Found ID", orgId)

					else:
						unMatchCount += 1
						self.displayNames(bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
							textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
							creativeCommons, electronicPub, None)

				else:
					print("\t*** NO LPTS")
					skipCount += 1
			else:
				print("\t SKIP typecode", typeCode)
				skipCount += 1

		print("total=", len(resultSet), " matched=", matchCount, " unmatched=", unMatchCount, " skiped=", skipCount)

	#### this is the way that it is done in lptsmanager
	###orgName in tempDic['Copyrightc']:

	def parseCopyright(self, copyright):
		if copyright != None:
			print("** input", copyright)
			pattern = re.compile(r".*([A-Za-z]+).*")
			match = pattern.match(copyright)
			if match != None:
				print("** output", match.group(1))
				return match.group(1)
		print("** output", "None")
		return None


	def displayNames(self, bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
		textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
		creativeCommons, electronicPub, parsedCopyright):
		print(bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId)
		if parsedCopyright != None:
			print("\tParsed Copyright", parsedCopyright)
		if textCopyright != None:
			print("\ttextCopyright", textCopyright)
		if audioCopyright != None:
			print("\taudioCopyright", audioCopyright)
		if videoCopyright != None:
			print("\tvideoCopyright", videoCopyright)
		if licensor != None:
			print("\tlicensor", licensor)
		if coLicensor != None:
			print("\tcoLicensor", coLicensor)
		if creativeCommons != None:
			print("\tcreativeCommons", creativeCommons)
		if electronicPub != None:
			print("\telectronicPub", electronicPub)
		for possibleName in self.possibleMapList[organizationId]:
			print("\tPossible: ", possibleName)		


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

