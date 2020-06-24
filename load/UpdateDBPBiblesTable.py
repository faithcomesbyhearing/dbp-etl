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
		## These scripts have multiple numeral systems.  So, this hack chooses the dominant one
		self.numeralIdMap["Arab"] = "eastern-arabic"
		self.numeralIdMap["Deva"] = "devanagari"
		## This one should be added to DBP
		self.numeralIdMap["Latn"] = "western-arabic"
		self.sizeCodeMap = self.db.selectMapList("SELECT id, set_size_code FROM bible_filesets WHERE set_type_code='text_format'", ())	
		self.yearPattern = re.compile("([0-9]+)")


	def findTextBibleLPTSRecords(self, bibleId, lptsReader):
		results = []
		lptsRecords = lptsReader.bibleIdMap.get(bibleId, [])
		for (lptsIndex, lptsRecord) in lptsRecords:
			damids = lptsRecord.DamIds("text", lptsIndex)
			if len(damids) > 0:
				results.append((lptsIndex, lptsRecord))
		return results


	def getBibleRecord(self, bibleId, lptsRecords):
		languageId = self.biblesLanguageId(bibleId, lptsRecords)
		versification = None
		script = self.biblesScript(bibleId, lptsRecords)
		numeralSystemId = self.biblesNumeralId(script)
		date = self.biblesDate(bibleId, lptsRecords)
		scope = self.biblesSizeCode(bibleId, lptsRecords)
		#derived = None
		copyright = self.biblesCopyright(bibleId, lptsRecords)
		#priority = self.biblesPriority(bibleId)
		reviewed = 0  ## I think this should be removed as well.  There is no way to update
		notes = None  ## I think this should be removed as well, or possible null out the value
		values = (languageId, versification, numeralSystemId, date,
			scope, script, copyright, reviewed, notes, bibleId)
		return values


	def biblesLanguageId(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			iso = lptsRecord.ISO()
			langName = lptsRecord.LangName()
			result = self.db.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			if result != None:
				final.add(result)
			else:
				result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
				if result != None:
					final.add(result)
		if len(final) == 0:
			iso = bibleId[:3].lower()
			result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
			if result != None:
				final.add(result)
		return self.findResult(bibleId, final, "LangName & ISO")


	def biblesScript(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			script = lptsRecord.Orthography(lptsIndex)
			if script != None:
				result = LookupTables.scriptCode(script)
				final.add(result)
		if len(final) == 0:
			return None
		elif len(final) == 1:
			return list(final)[0]
		else:
			if "Latn" in final:
				final.remove("Latn")
			if len(final) == 1:
				return list(final)[0]
			else:
				print("WARN: bible_id %s has multiple scripts |%s|" % (bibleId, "|".join(final)))
				return list(final)[0]


	def biblesNumeralId(self, script):
		numeralSystemId = self.numeralIdMap.get(script) 
		return numeralSystemId


	def biblesDate(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			self.extractYear(final, lptsRecord.Copyrightc())
		if len(final) == 0:
			for (lptsIndex, lptsRecord) in lptsRecords:
				self.extractYear(final, lptsRecord.Volumne_Name())
		return self.findResult(bibleId, final, "Copyrightc Date")


	def biblesSizeCode(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			damIds = lptsRecord.DamIds("text", lptsIndex)
			#print(damIds)
			for damId in damIds:
				sizeCodes = self.sizeCodeMap.get(damId, [])
				for sizeCode in sizeCodes:
					if sizeCode != None:
						final.add(sizeCode)
		if len(final) == 0:
			return None
		elif len(final) == 1:
			return list(final)[0]
		else:
			## reduce multiple sizeCodes to one as a union
			hasNT = False
			hasOT = False
			hasNTP = False
			hasOTP = False
			for sizeCode in final:
				if "NTP" in sizeCode:
					hasNTP = True
				elif "NT" in sizeCode:
					hasNT = True
				if "OTP" in sizeCode:
					hasOTP = True
				elif "OT" in sizeCode:
					hasOT = True
				if "C" in sizeCode:
					hasNT = True
					hasOT = True
			result = ""
			if hasNT:
				result += "NT"
			elif hasNTP:
				result += "NTP"
			if hasOT:
				result += "OT"
			elif hasOTP:
				result += "OTP"
			return result


	def extractYear(self, finalSet, value):
		if value != None:
			match = self.yearPattern.search(value)
			if match != None:
				finalSet.add(match.group(1))


	def biblesCopyright(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			result = lptsRecord.Copyrightc()
			if result != None:
				final.add(result)
		return self.findResult(bibleId, final, "Copyrightc")


#	def biblesPriority(self, bibleId):
#		priority = {"ENGESV": 20,
#					"ENGNIV": 19,
#					"ENGKJV": 18,
#					"ENGCEV": 17,
#					"ENGNRSV": 16,
#					"ENGNAB": 15,
#					"ENGWEB": 14,
#					"ENGNAS": 13,
#					"ENGNLV": 11,
#					"ENGEVD": 10}
#		return priority.get(bibleId, 0)


	def findResult(self, bibleId, final, fieldName):
		if len(final) == 0:
			return None
		elif len(final) == 1:
			return list(final)[0]
		else:
			print("WARN: bible_id %s has multiple %s values |%s|" % (bibleId, fieldName, "|".join(final)))
			return list(final)[0]


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	bibleIds = db.selectList("SELECT distinct bible_id FROM bible_fileset_connections", ())
	db.close()
	lptsReader = LPTSExtractReader(config)
	update = UpdateDBPBiblesTable(config)
	update.counts = 0
	for bibleId in bibleIds:
		lptsRecords = update.findTextBibleLPTSRecords(bibleId, lptsReader)
		values = update.getBibleRecord(bibleId, lptsRecords)
		#print(values)
	print("count", update.counts)

