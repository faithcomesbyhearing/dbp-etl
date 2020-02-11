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
		self.textCopyrightSet = set()
		self.audioCopyrightSet = set()
		self.videoCopyrightSet = set()
		self.licensorSet = set()
		self.coLicensorSet = set()
		self.stripChars = r"[.,©℗\d/\-_\(\)]"


	def process(self, lptsReader):
		orgIdMap = {}
		## This logic assumes there are no names with multiple org ids
		resultSet = self.db.select("SELECT name, organization_id FROM organization_translations", ())
		for row in resultSet:
			name = row[0].lower()
			#name = name.replace("la ", "")
			#name = name.replace(" ", "")
			#name = name.replace("the", "")
			#name = row[0].replace("the")
			#name = row[0].strip()
			orgIdMap[name.strip()] = row[1]
		
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
						self.addAudioCopyright(record.Copyrightp())
					elif typeCode == "audio" and organizationRole == 2:
						orgName = self.parseLicensor(record.Licensor())
						self.addLicensor(record.Licensor())
					elif typeCode == "audio" and organizationRole == 3:
						orgName = "I don't know"
					elif typeCode == "text" and organizationRole == 1:
						orgName = self.parseCopyright(record.Copyrightc())
						self.addTextCopyright(record.Copyrightc())
					elif typeCode == "text" and organizationRole == 2:
						orgName = self.parseLicensor(record.Licensor())
						self.addLicensor(record.Licensor())
					elif typeCode == "text" and organizationRole == 3:
						orgName = "I don't know"
					elif typeCode == "video" and organizationRole == 1:
						orgName = self.parseCopyright(record.Copyright_Video())
						self.addVideoCopyright(record.Copyright_Video())
					elif typeCode == "video" and organizationRole == 2:
						orgName = self.parseLicensor(record.licensor())
						self.addLicensor(record.Licensor())
					elif typeCode == "video" and organizationRole == 3:
						orgName = "I don't know"
					else:
						print("ERROR: unexpected typeCode", typeCode)
						sys.exit()

					orgId = self.matchName(orgIdMap, orgName)
					if orgId != None and orgId == organizationId:
						matchCount += 1
					else:
						unMatchCount += 1
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
		for copyright in sorted(self.textCopyrightSet):
			print("text: |%s|" % (copyright))
		for copyright in sorted(self.audioCopyrightSet):
			print("audio: |%s|" % (copyright))
		for copyright in sorted(self.videoCopyrightSet):
			print("video: |%s|" % (copyright))
		for licensor in sorted(self.licensorSet):
			print("licensor: |%s|" % (licensor))


	def parseCopyright(self, copyright):
		if copyright != None:
			return copyright.lower()
		else:
			return None


	def parseLicensor(self, licensor):
		print("** input ", licensor)
		if licensor != None:
			parts = licensor.lower().split(",", 2)
			if len(parts) > 1:
				print("** output", parts[1] + " " + parts[0])
				return parts[1] + " " + parts[0]
			else:
				return parts[0]
		return None

	def matchName(self, orgIdMap, orgName):
		if orgName != None:
			if orgName == "sil":
				return 30
			for name in orgIdMap.keys():
				if name in orgName:
					orgId = orgIdMap[name]
					if orgId==144:   # bible-society-of-uganda
						return 745   # the-bible-society-of-uganda
					elif orgId==72:  # bible-society-of-ghana
						return 727   # the-bible-society-of-ghana
					elif orgId==127: # bible-society-of-suriname
						return 750   # bibles-international
					elif orgId==527: # sim
						return 20    # sim-serving-in-mission
					#elif orgId == 4: # biblica
					#	return 783	 # the-bible-league
					else:
						return orgId
		return None

	def addTextCopyright(self, copyright):
		if copyright != None:
			self.textCopyrightSet.add(re.sub(self.stripChars, "", copyright).strip().replace("   ", " ").replace("  ", " "))

	def addAudioCopyright(self, copyright):
		if copyright != None:
			self.audioCopyrightSet.add(re.sub(self.stripChars, "", copyright).strip().replace("   ", " ").replace("  ", " "))

	def addVideoCopyright(self, copyright):
		if copyright != None:
			self.videoCopyrightSet.add(re.sub(self.stripChars, "", copyright).strip().replace("   ", " ").replace("  ", " "))

	def addLicensor(self, licensor):
		if licensor != None:
			self.licensorSet.add(re.sub(self.stripChars, "", licensor).strip().replace("   ", " ").replace("  ", " "))

	def addCoLicensor(self, licensor):
		if licensor != None:
			self.licensorSet.add(re.sub(self.stripChars, "", licensor).strip().replace("   ", " ").replace("  ", " "))

#	def displayNames(self, bibleId, filesetId, hashId, typeCode, organizationRole, slug, organizationId,
#		textCopyright, audioCopyright, videoCopyright, licensor, coLicensor,
#		creativeCommons, electronicPub, parsedOrgName):
#		print("DBP: %s/%s/%s hash:%s, role: %s, orgId: %s, orgSlug: %s" %(typeCode, bibleId, filesetId, hashId, organizationRole, organizationId, slug))
#		if textCopyright != None:
#			print("\tLPTS Copyrightc:", textCopyright)
#		if audioCopyright != None:
#			print("\tLPTS Copyrightp:", audioCopyright)
#		if videoCopyright != None:
#			print("\tLPTS Copyright_Video:", videoCopyright)
#		if licensor != None:
#			print("\tLPTS Licensor:", licensor)
#		if coLicensor != None:
#			print("\tLPTS CoLicensor:", coLicensor)
#		if creativeCommons != None:
#			print("\tLPTS CreativeCommonsText:", creativeCommons)
#		if electronicPub != None:
#			print("\tLPTS ElectronicPublisher:", electronicPub)
#		for possibleName in self.possibleMapList[organizationId]:
#			print("\torganization_translations.name for %s: %s" % (organizationId, possibleName))		


if (__name__ == '__main__'):
	config = Config()
	lptsReader = LPTSExtractReader(config)
	organizations = Organizations(config)
	organizations.process(lptsReader)

