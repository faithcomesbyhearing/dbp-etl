# UpdateDBPBiblesTable.py
#
# This program inserts and replaces records in the DBP bibles table
#
# This class is intended to run before the update of filesets as a result of a load.
# It updates the bibles table based upon LPTS content, without regard to which Bibles
# have been uploaded.  When the filesets are updated, the bible_fileset_connections are
# also updated.


import io
import sys
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from LPTSExtractReader import *
from LookupTables import *


class UpdateDBPBiblesTable:


	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader
		self.numeralIdMap = self.db.selectMap("SELECT script_id, numeral_system_id FROM alphabet_numeral_systems", ())
		## These scripts have multiple numeral systems.  So, this hack chooses the dominant one
		self.numeralIdMap["Arab"] = "eastern-arabic"
		self.numeralIdMap["Deva"] = "devanagari"
		## This one should be added to DBP
		self.numeralIdMap["Latn"] = "western-arabic"
		self.sizeCodeMap = self.db.selectMapList("SELECT id, set_size_code FROM bible_filesets", ())	
		self.yearPattern = re.compile("([0-9]+)")


	##
	## Bibles Table
	##
	def process(self):
		insertRows = []
		updateRows = []
		deleteRows = []

		## select bibles from DBP
		dbpBibleMap = {}
		sql = "SELECT id, language_id, versification, numeral_system_id, `date`, scope, script FROM bibles"
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			dbpBibleMap[row[0]] = row[1:]

		## retrieve bibles from LPTS
		lptsBibleMap = self.lptsReader.getBibleIdMap()

		for dbpBibleId in sorted(dbpBibleMap.keys()):
			if dbpBibleId not in lptsBibleMap.keys():
				deleteRows.append(dbpBibleId)

		for bibleId in sorted(lptsBibleMap.keys()):
			lptsRecords = lptsBibleMap[bibleId]
			languageId = self.biblesLanguageId(bibleId, lptsRecords)
			versification = None
			script = self.biblesScript(bibleId, lptsRecords)
			numerals = self.biblesNumeralId(script)
			date = self.biblesDate(bibleId, lptsRecords)
			scope = self.biblesSizeCode(bibleId, lptsRecords)

			if bibleId not in dbpBibleMap.keys():
				insertRows.append((languageId, versification, numerals, date, scope, script, bibleId))
			else:
				(dbpLanguageId, dbpVersification, dbpNumerals, dbpDate, dbpScope, dbpScript) = dbpBibleMap[bibleId]
				if (languageId != dbpLanguageId or
					versification != dbpVersification or
					numerals != dbpNumerals or
					date != dbpDate or
					scope != dbpScope or
					script != dbpScript):
					updateRows.append((languageId, versification, numerals, date, scope, script, bibleId))

		tableName = "bibles"
		pkeyNames = ("id",)
		attrNames = ("language_id", "versification", "numeral_system_id", "date", "scope", "script")
		ignoredNamed = ("derived", "copyright", "priority", "reviewed", "notes") # here for doc only
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def biblesLanguageId(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			iso = lptsRecord.ISO()
			langName = lptsRecord.LangName()
			result = self.db.selectScalar("SELECT l.id FROM languages l,language_translations t WHERE l.iso=%s AND t.name=%s AND l.id=t.language_source_id", (iso, langName))
			if result != None:
				final.add(result)
		if len(final) == 0:
			for (lptsIndex, lptsRecord) in lptsRecords:
				iso = lptsRecord.ISO()
				result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
				if result != None:
					final.add(result)
		if len(final) == 0:
			iso = lptsRecord.ISO()
			result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
			if result != None:
				final.add(result)
		if len(final) == 0:
			return None
		if len(final) > 1:
			print("ERROR_01 Duplicate language_id for bibleId", bibleId, final)
		return list(final)[0]


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
			for typeCode in ["audio", "text", "video"]:
				damIds = lptsRecord.DamIdMap(typeCode, lptsIndex)
				#print("DAMIDS", damIds)
				for damId in damIds.keys():
					sizeCodes = self.sizeCodeMap.get(damId, [])
					for sizeCode in sizeCodes:
						if sizeCode != None:
							final.add(sizeCode)
		#print("FOUND SIZES", final)
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
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	bibles = UpdateDBPBiblesTable(config, db, dbOut, lptsReader)
	bibles.process()
	db.close()

	dbOut.displayStatements()
	dbOut.displayCounts()

