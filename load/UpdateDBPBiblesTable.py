# UpdateDBPBiblesTable.py
#
# This program inserts and replaces records in the DBP bibles table
#

import io
import sys
from Config import *
from SQLUtility import *
from LPTSExtractReader import *
from LookupTables import *


class UpdateDBPBiblesTable:


	def __init__(self, config):
		self.config = config
		self.db = SQLUtility(config)
		self.numeralIdMap = self.db.selectMap("SELECT script_id, numeral_system_id FROM alphabet_numeral_systems", ())
		## This should be added to DBP
		self.numeralIdMap["Latn"] = "western-arabic"


	def getBibleRecord(self, bibleId, lptsRecord, lptsIndex, setSizeCode):
		languageId = self.biblesLanguageId(bibleId, lptsRecord)
		versification = self.biblesVersification(bibleId, lptsRecord, lptsIndex)
		script = self.biblesScript(bibleId, lptsRecord, lptsIndex)
		numeralSystemId = self.biblesNumeralId(script)
		date = self.biblesDate(lptsRecord)
		scope = setSizeCode
		derived = None
		copyright = self.biblesCopyright(lptsRecord)
		priority = self.biblesPriority(bibleId)
		reviewed = 0
		notes = None
		values = [(languageId, versification, numeralSystemId, date,
			scope, script, derived, copyright, priority, reviewed, notes, bibleId)]
		return values


	def biblesLanguageId(self, bibleId, lptsRecord):
		result = None
		if lptsRecord != None:
			iso = lptsRecord.ISO()
			langName = lptsRecord.LangName()
			result = self.db.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			if result != None:
				return result
			result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
			if result != None:
				return result
		else:
			iso = bibleId[:3].lower()
		result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
		return result


	def biblesVersification(self, bibleId, lptsRecord, lptsIndex):
		return None


	def biblesScript(self, bibleId, lptsRecord, lptsIndex):
		result = None
		script = lptsRecord.Orthography(lptsIndex)
		if script != None:
			result = LookupTables.scriptCode(script)
		return result


	def biblesNumeralId(self, script):
		print(script)
		## Bug: Arab and Deva have 2 numeral systems and I have no way to choose
		numeralSystemId = self.numeralIdMap.get(script) 
		print(self.numeralIdMap)
		print(numeralSystemId)
		return numeralSystemId


	def biblesDate(self, lptsRecord):
		volume = lptsRecord.Volumne_Name()
		if volume != None:
			yearPattern = re.compile("([0-9]+)")
			match = yearPattern.search(volume)
			if match != None:
				return match.group(1)
		return None


	def biblesCopyright(self, lptsRecord):
		return lptsRecord.Copyrightc()


	def biblesPriority(self, bibleId):
		priority = {"ENGESV": 20,
					"ENGNIV": 19,
					"ENGKJV": 18,
					"ENGCEV": 17,
					"ENGNRSV": 16,
					"ENGNAB": 15,
					"ENGWEB": 14,
					"ENGNAS": 13,
					"ENGNLV": 11,
					"ENGEVD": 10}
		return priority.get(bibleId, 0)


	def findTextBibleLPTSRecord(self, bibleId, lptsReader):
		lptsRecords = lptsReader.bibleIdMap.get(bibleId)
		if len(lptsRecords) == 0:
			print("ERROR: %s has not LPTS records" % (bibleId))
			sys.exit()
		results = []
		for (lptsIndex, lptsRecord) in lptsRecords:
			damids = lptsRecord.DamIds("text", lptsIndex)
			if len(damids) > 0:
				results.append((lptsIndex, lptsRecord))
		if len(results) == 1:
			return results[0]
		elif len(results) > 1:
			for (index, record) in results:
				print(index, record.DBP_Equivalent(), record.Reg_StockNumber())
			print("ERROR: %s has too many LPTS text records" % (bibleId))
			sys.exit()
		results = []
		print("Starting loose select for %s" % (bibleId))
		print("We need a loose procedure")
		sys.exit()


## Unit Test
if (__name__ == '__main__'):
	#bibleId = "ENGESV"
	#bibleId = "ENGKJV"
	#bibleId = "ENGASV"
	bibleId = "ENGCEV"
	setSizeCode = "NTOT"
	config = Config()
	lptsReader = LPTSExtractReader(config)
	update = UpdateDBPBiblesTable(config)
	(lptsIndex, lptsRecord) = update.findTextBibleLPTSRecord(bibleId, lptsReader)
	values = update.getBibleRecord(bibleId, lptsRecord, lptsIndex, setSizeCode)
	print(values)

