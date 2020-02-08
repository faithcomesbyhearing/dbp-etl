# Organizations.py

# This program brings to one place copyright organization data from DBP
# And the LPTS fields used to populate that data.  The purpose of this
# program was to reverse engineer how organization data is loaded into DBP

import io
import os
import sys
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
		sql = ("SELECT bf.id AS fileset_id, bf.hash_id, bf.set_type_code, bfco.organization_role,"
			" o.slug, o.id AS organization_id, ot.name, bfc.bible_id"
			" FROM bible_filesets bf"
			" JOIN bible_fileset_copyright_organizations bfco ON bfco.hash_id = bf.hash_id"
			" JOIN organizations o ON bfco.organization_id = o.id"
			" JOIN organization_translations ot ON o.id = ot.organization_id"
			" JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id"
			" ORDER BY bf.id, bf.hash_id")
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			filesetId = row[0]
			hashId = row[1]
			setTypeCode = row[2]
			typeCode = setTypeCode.split("_")[0]
			organizationRole = row[3]
			slug = row[4]
			organizationId = row[5]
			name = row[6]
			bibleId = row[7]
			if typeCode in {"audio", "text", "video"}:
				print(filesetId, hashId, typeCode, organizationRole, slug, organizationId, name, bibleId)

				#lptsRecord 
				(record, index, status) = lptsReader.getLPTSRecord(typeCode, bibleId, filesetId)
				if record != None:
					textCopyright = record.Copyrightc()
					audioCopyright = record.Copyrightp()
					videoCopyright = record.Copyright_Video()
					licensor = record.Licensor()
					coLicensor = record.CoLicensor()
					creativeCommons = record.CreativeCommonsText()
					electronicPub = record.ElectronicPublisher(index)
					print("\ttextCopyright", textCopyright)
					print("\taudioCopyright", audioCopyright)
					print("\tvideoCopyright", videoCopyright)
					print("\tlicensor", licensor)
					print("\tcoLicensor", coLicensor)
					print("\tcreativeCommons", creativeCommons)
					print("\telectronicPub", electronicPub)
				else:
					print("\t*** NO LPTS")
			else:
				print("\t SKIP typecode", typeCode)

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