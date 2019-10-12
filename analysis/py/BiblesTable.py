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
from SQLUtility import *
from LPTSExtractReader import *
from LookupTables import *

class BiblesTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()
		print("num bibles in dbp-prod + verses", len(bibles.bibleIds))
		results = []
		for bibleId in self.bibleIds:
			bible = self.bibleMap.get(bibleId)
			lang = self.languageId(bibleId, bible)
			verse = self.versification(bible)
			date = self.date(bible)
			scope = self.scope(bible)
			script = self.script(bible)
			numeral = self.numeralSystemId(script)
			derived = self.derived(bible)
			copyright = self.copyright(bible)
			priority = self.priority(bible)
			reviewed = self.reviewed(bible)
			notes = self.notes(bible)
			results.append((bibleId, lang, verse, numeral, date, scope, script, derived, copyright, 
				priority, reviewed, notes))

		print("num bibles to be inserted: %d" % (len(results)))
		self.db.executeBatch("INSERT INTO bibles (id, language_id, versification, numeral_system_id, `date`, scope, script, derived, copyright, priority, reviewed, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", results)


	def readAll(self):
		self.bibleIds = self.db.selectSet("SELECT distinct bible_id FROM bucket_verse_summary ORDER BY bible_id", None)
		print("num %d bibleIds in bucket_verse_summary" % (len(self.bibleIds)))

		reader = LPTSExtractReader(self.config)
		self.bibleMap = reader.getBibleIdMap()
		print("num bibles in map", len(self.bibleMap.keys()))


	def languageId(self, bibleId, bible):
		result = None
		if bible != None:
			iso = bible.ISO()
			langName = bible.LangName()
			#result = self.db.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
			if result != None:
				return result
		else:
			iso = bibleId[:3].lower()
		result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
		if result != None:
			return result
		else:
			return "7946" # Null is not allowed THIS SHOULD BE A VALIDATION WARNING


	def versification(self, bible):
		result = 'protestant'
		return result
		# ask alan for source 9/16/19


	def numeralSystemId(self, script):
		## associating this with Bible is incorrect, because there could be multiple
		## damIds with different scripts
		result = None
		result = self.db.selectScalar("SELECT numeral_system_id FROM alphabet_numeral_systems WHERE script_id=%s", (script))
		## note this query returns multiple rows for Arab and Deva, I dont know which is correct
		if result != None:
			return result
		else:
			return 'western-arabic' ## this default value is not found in the alphabet_numeral_systems table


	def date(self, bible):
		result = None
		#print("this appears to be a copyright date") # need correct source from 
		return result


	def scope(self, bible):
		result = None
		#print("should be the same as bible_filesets.set_size_code see fileset.py 627-647")
		return result


	def script(self, bible):
		result = None
		if bible != None:
			script = bible.x0031_Orthography()
			if script == None:
				script = bible.x0032_Orthography()
				if script == None:
					script = bible.x0033_Orthography()
			if script != None:
				lookup = LookupTables()
				result = lookup.scriptCode(script)
		if result != None:
			return result
		else:
			return 'Zzzz' # cannot be null, THIS SHOULD BE A VALIDATION WARNING


	def derived(self, bible):
		result = None
		# should be a note about the version this is derived from
		return result


	def copyright(self, bible):
		result = None
		if bible != None:
			copyc = bible.Copyrightc()
			if len(copyc) > 191:
				result = copyc[:190]
				print("WARNING: Copyright truncated for %s" % (bible.DBP_Equivalent()))
			else:
				result = copyc
		return result


	def priority(self, bible):
		result = 0
		# I have no idea what this is.  It is a value between 0 and 20. Almost all are zero
		return result


	def reviewed(self, bible):
		result = 1
		# This is a boolean 0 or 1. 1 is by far the most common
		return result


	def notes(self, bible):
		# This was used for only a couple rows. It could be ignored
		return None


config = Config()
db = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
bibles = BiblesTable(config, db)
bibles.process()
db.close()




