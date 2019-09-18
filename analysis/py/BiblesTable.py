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
from BucketReader import *
from LPTSExtractReader import *
from SQLUtility import *
from LookupTables import *

class BiblesTable:

	def __init__(self, config):
		bucket = BucketReader(config)
		self.bibleIds = bucket.bibleIds()
		self.inputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_input_db_name)
		reader = LPTSExtractReader(config)
		self.bibleMap = reader.getBibleIdMap()
		print("num bibles in map", len(self.bibleMap.keys()))


	def languageId(self, bible):
		result = 7946 # Null is not allowed THIS SHOULD BE A VALIDATION WARNING
		iso = bible['ISO']
		langName = bible['LangName']
		#print("doing languageId", bibleId, iso, langName)
		#result = self.inputDB.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
		result = self.inputDB.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
		if result == None:
			result = self.inputDB.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
		return result


	def versification(self, bible):
		#print("versification")
		result = 'protestant'
		return result
		# ask alan for source 9/16/19


	def numeralSystemId(self, bible):
		# associating this with Bible is incorrect, because there could be multiple
		# damIds with different scripts
		result = 'western-arabic' # this default value is not found in the alphabet_numeral_systems table
		script = bible.get('_x0031_Orthography') # really there is supposed to be a linkage
		if script == None:
			script = bible.get('_x0032_Orthography')
			if script == None:
				script = bible.get('_x0033_Orthography')
		if script != None:
			lookup = LookupTables()
			scriptCode = lookup.scriptCode(script)
			if scriptCode == None:
				print("ERROR: missing script code for %s" % scriptCode)
				sys.exit()
			ans = self.inputDB.selectScalar("SELECT numeral_system_id FROM alphabet_numeral_systems WHERE script_id=%s", (scriptCode))
			result = ans if ans != None else 'western-arabic'
			# note this query returns multiple rows for Arab and Deva, I dont know which is correct
		return result


	def date(self, bible):
		result = None
		#print("this appears to be a copyright date") # need correct source from 
		return result


	def scope(self, bible):
		result = None
		#print("should be the same as bible_filesets.set_size_code see fileset.py 627-647")
		return result


	def script(self, bible):
		result = 'Zzzz' # cannot be null, THIS SHOULD BE A VALIDATION WARNING
		script = bible.get('_x0031_Orthography') # really there is supposed to be a linkage
		if script == None:
			script = bible.get('_x0032_Orthography')
			if script == None:
				script = bible.get('_x0033_Orthography')
		if script != None:
			lookup = LookupTables()
			result = lookup.scriptCode(script)
		return result


	def derived(self, bible):
		result = None
		# should be a note about the version this is derived from
		return result


	def copyright(self, bible):
		result = None
		copyc = bible.get('Copyrightc')
		if len(copyc) > 191:
			result = copyc[:190]
			print("WARNING: Copyright truncated for %s" % (bible.get("DBP_Equivalent")))
		else:
			result = copyc
		return result


	def priority(self, bible):
		result = 0
		# I have no idea what this is.  It is a value between 0 and 20. Almost all are zero
		return result


	def reviewed(self, bible):
		result = None
		# This is a boolean 0 or 1. 1 is by far the most common
		return result


	def notes(self, bible):
		# This was used for only a couple rows. It could be ignored
		return None


config = Config()
bibles = BiblesTable(config)
print("num bibles in dbp-prod", len(bibles.bibleIds))
results = []
for bibleId in bibles.bibleIds:
	bible = bibles.bibleMap.get(bibleId)
	if bible != None:
		lang = bibles.languageId(bible)
		verse = bibles.versification(bible)
		numeral = bibles.numeralSystemId(bible)
		date = bibles.date(bible)
		scope = bibles.scope(bible)
		script = bibles.script(bible)
		derived = bibles.derived(bible)
		copyright = bibles.copyright(bible)
		priority = bibles.priority(bible)
		reviewed = bibles.reviewed(bible)
		notes = bibles.notes(bible)
		results.append((bibleId, lang, verse, numeral, date, scope, script, derived, copyright, 
			priority, reviewed, notes))
	else:
		print("WARNING LPTS has no record for %s" % (bibleId))

bibles.inputDB.close()
outputDB = SQLUtility(config.database_host, config.database_port,
			config.database_user, config.database_output_db_name)
outputDB.executeBatch("INSERT INTO bibles (id, language_id, versification, numeral_system_id, `date`, scope, script, derived, copyright, priority, reviewed, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", results)
outputDB.close()



