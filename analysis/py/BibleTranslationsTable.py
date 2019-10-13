# BibleTranslationsTable.py
#
# This table contains 14 (28) rows that are entirely duplicated. The logical primary key is unknown.
#
# There are 118 (236) rows that are duplicates except for the vernacular flag. They are all 6414 language_
# The table needs a unique index in order to define a uniqueness constaint.  It seems like, bible_id, vernacular
# is the correct key.
#
# It is strange that language_id is in this table as well as bibles, because that permits multiple
# language_id's per bibleId, which would contradict the bibles table.  It should be in one of the
# two tables, not both.
#
#| id               | int(10) unsigned | NO   | PRI | NULL              | auto_increment              |
#| language_id      | int(10) unsigned | NO   | MUL | NULL              |                             |
#| bible_id         | varchar(12)      | NO   | MUL | NULL              |                             |
#| vernacular       | tinyint(1)       | NO   |     | 0                 |                             |
#| vernacular_trade | tinyint(1)       | NO   |     | 0                 |                             |
#| name             | varchar(191)     | NO   |     | NULL              |                             |
#| description      | text             | YES  |     | NULL              |                             |
#| background       | text             | YES  |     | NULL              |                             |
#| notes            | text             | YES  |     | NULL              |                             |
#| PRIMARY KEY (id)
#| KEY (language_id)
#| KEY (bible_id)
#| FOREIGN KEY (bible_id) REFERENCES bibles (id)
#| FOREIGN KEY (language_id) REFERENCES languages (id)
#
# This table has no unique logical key.  bible_id, language_id, vernacular is close to unique.


import io
import os
import sys
from Config import *
from LPTSExtractReader import *
from SQLUtility import *


class BibleTranslationsTable:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.readAll()
		print("num bibles in dbp-prod", len(self.bibleIds))
		results = []
		for bibleId in self.bibleIds:
			bible = self.bibleMap.get(bibleId)
			lang = self.languageId(bibleId, bible)
			#lang = biblesTable.languageId(bibleId, bible)
			vernacular = self.vernacular()
			vernacularTrade = self.vernacularTrade()
			name = self.name(bible)
			description = self.description()
			background = self.background()
			notes = self.notes()
			results.append((lang, bibleId, vernacular, vernacularTrade, name, description, background, notes))

		self.db.executeBatch("INSERT INTO bible_translations (language_id, bible_id, vernacular, vernacular_trade, name, description, background, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", results)	


	def readAll(self):
		self.bibleIds = self.db.selectSet("SELECT distinct bible_id FROM bucket_verse_summary ORDER BY bible_id", None)
		reader = LPTSExtractReader(self.config)
		self.bibleMap = reader.getBibleIdMap()
		print("num bibles in map", len(self.bibleMap.keys()))


	## must change to use BibleTable.languageId function
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


	def vernacular(self):
		# I have not idea what the source of this column is.  It is not updated by lptsmanager
		# and yet, there are some recent rows that are updated from the default.
		# I suspect that lptsmanager only creates records with a 0 vernacular, and some other
		# process is adding records with a vernacular of 1.
		result = 0
		return result


	def vernacularTrade(self):
		# There is only 1 record where this field is set to 1 and it was inserted
		# immediately after an equivalent record, but with different language_id
		# inserted a record with vernacular_trade 0.
		result = 0
		return result


	def name(self, bible):
		result = None
		if bible != None:
			result = bible.HeartName()
			#	else:
			#	result = ""
			#return result
			if result == None:
				#result = bible.get("Volumne_Name") # lptsmanager used this column
				result = bible.Volumne_Name()
		if result != None:
			return result
		else:
			return ""


	def description(self):
		# This column contains a strange assortment of description with no consistent
		# type of content.  Hence its usefulness is questionable.
		# but, it could be derived from the languages table, but why bother.
		result = None
		return result


	def background(self):
		# similar to description, but fewer entires
		result = None
		return result


	def notes(self):
		# There is only 1 row with notes
		result = None
		return result


config = Config()
db = SQLUtility(config.database_host, config.database_port,
				config.database_user, config.database_output_db_name)
bibles = BibleTranslationsTable(config, db)
bibles.process()
db.close()




