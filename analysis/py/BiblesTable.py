# BiblesTable.py

#
# This program reads dbp-prod listing to get a unique list of sorted bible_ids.
# It then looks up each of these in the LPTS extract and pulls in the additional
# data to complete the bibles table.
#
#| id                | varchar(12)         | NO   | PRI | NULL              |                             |
#| language_id       | int(10) unsigned    | NO   | MUL | NULL              |                             |
#| versification     | varchar(20)         | NO   |     | protestant        |                             |
#| numeral_system_id | varchar(20)         | NO   | MUL | NULL              |                             |
#| date              | varchar(191)        | YES  |     | NULL              |                             |
#| scope             | char(4)             | YES  |     | NULL              |                             |
#| script            | char(4)             | NO   | MUL | NULL              |                             |
#| derived           | text                | YES  |     | NULL              |                             |
#| copyright         | varchar(191)        | YES  |     | NULL              |                             |
#| priority          | tinyint(3) unsigned | NO   |     | 0                 |                             |
#| reviewed          | tinyint(1)          | YES  |     | 0                 |                             |
#| notes             | text                | YES  |     | NULL  
#

import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *

class BiblesTable:

	def __init__(self, config):
		ids = set()
		files = io.open(config.directory_main_bucket, mode="r", encoding="utf-8")
		for line in files:
			parts = line.split("/")
			if parts[0] in ["audio", "text", "video"]:
				if len(parts) > 3:
					bibleId = parts[1]
					#print(bibleId)
					ids.add(bibleId)
		files.close()
		self.bibleIds = sorted(list(ids))
		self.inputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_input_db_name)
		reader = LPTSExtractReader(config)
		self.bibleMap = reader.getBibleIdMap()
		print("num bibles in map", len(self.bibleMap.keys()))

	def languageId(self, bibleId):
		bidMap = self.bibleMap.get(bibleId)
		if bidMap != None:
			iso = bidMap['ISO']
			langName = bidMap['LangName']
			print("doing languageId", bibleId, iso, langName)
			result = self.inputDB.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			if result == None:
				result = self.inputDB.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
		else:
			result = None
		return result




config = Config()
bibles = BiblesTable(config)
print("num bibles in dbp-prod", len(bibles.bibleIds))
results = []
for bid in bibles.bibleIds:
	lang = bibles.languageId(bid)
	print("lang", bid, lang)
	results.append((bid, lang))

#	print(item)

#what should be the logic sequence?
# create one method for each data column that processes the column for one item
# have a main that iterates over the bibleIds and calls each column
