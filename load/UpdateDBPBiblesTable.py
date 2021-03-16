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


class UpdateDBPBiblesTable:


	def __init__(self, config, db, dbOut, lptsReader):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.lptsReader = lptsReader
		self.filesetConnectionSet = self.db.selectSet("SELECT distinct bible_id FROM bible_fileset_connections", ())
		sql = ("SELECT l.iso, a.script_id FROM alphabet_language a"
				" JOIN languages l ON a.language_id = l.id"
				" WHERE a.script_id IN" 
				" (SELECT a.script_id FROM alphabet_language a"
				" JOIN languages l ON a.language_id = l.id"
				" GROUP BY a.script_id"
				" having count(*) = 1)")
		self.scriptCodeMap = self.db.selectMap(sql, ())
		self.scriptNameMap = self.db.selectMap("SELECT lpts_name, script_id FROM lpts_script_codes", ())
		self.numeralIdSet = self.db.selectSet("SELECT id FROM numeral_systems", ())
		self.sizeCodeMap = self.db.selectMapList("SELECT id, set_size_code FROM bible_filesets", ())	
		resultSet = self.db.select("SELECT lower(l.iso), lower(t.name), l.id FROM languages l,language_translations t"
			" WHERE l.id=t.language_source_id AND priority = 9 ORDER BY l.id desc", ())
		self.languageMap1 = {}
		for (iso, name, langId) in resultSet:
			self.languageMap1[(iso, name)] = langId
		resultSet = self.db.select("SELECT lower(iso), lower(name), id FROM languages", ())# ORDER BY id desc", ())
		self.languageMap2 = {}
		for (iso, name, langId) in resultSet:
			self.languageMap2[(iso, name)] = langId
		self.languageMap3 = self.db.selectMap("SELECT lower(iso), id FROM languages ORDER BY id desc", ())
		self.yearPattern = re.compile("([0-9]{4})")


	##
	## Bibles Table
	##
	def process(self):
		insertRows = []
		updateRows = []
		deleteRows = []

		## select bibles from DBP
		dbpBibleMap = {}
		sql = "SELECT id, language_id, versification, numeral_system_id, `date`, scope, script, copyright FROM bibles"
		resultSet = self.db.select(sql, ())
		for row in resultSet:
			dbpBibleMap[row[0]] = row[1:]

		## retrieve bibles from LPTS
		lptsBibleMap = self.lptsReader.getBibleIdMap()
		lptsBibleMap.pop("JESUS FILM", None) # delete JESUS FILM

		for dbpBibleId in sorted(dbpBibleMap.keys()):
			if dbpBibleId not in lptsBibleMap.keys():
				if dbpBibleId in self.filesetConnectionSet:
					print("WARN Attempt to delete bible_id: %s when it has filesets." % (dbpBibleId))
				elif not self.testBibleForeignKeys(dbpBibleId):
					deleteRows.append(dbpBibleId)

		for bibleId in sorted(lptsBibleMap.keys()):
			lptsRecords = lptsBibleMap[bibleId]
			languageId = self.biblesLanguageId(bibleId, lptsRecords)
			#versification = self.biblesVersification(bibleId, lptsRecords)
			script = self.biblesScript(bibleId, lptsRecords)
			numerals = self.biblesNumeralId2(bibleId, lptsRecords)
			scope = self.biblesSizeCode(bibleId, lptsRecords)
			copyright = self.bibleTextCopyright(bibleId, lptsRecords)
			date = self.biblesDate(copyright)

			if bibleId not in dbpBibleMap.keys():
				if languageId != None:
					if copyright != None:
						copyright = copyright.replace("'", "''")
					insertRows.append((languageId, "protestant", numerals, date, scope, script, copyright, bibleId))
			else:
				(dbpLanguageId, dbpVersification, dbpNumerals, dbpDate, dbpScope, dbpScript, dbpCopyright) = dbpBibleMap[bibleId]
				if languageId != dbpLanguageId and languageId != None:
					updateRows.append(("language_id", languageId, dbpLanguageId, bibleId))
				if numerals != dbpNumerals:
					updateRows.append(("numeral_system_id", numerals, dbpNumerals, bibleId))
				if date != dbpDate:
					updateRows.append(("date", date, dbpDate, bibleId))
				if scope != dbpScope:
					updateRows.append(("scope", scope, dbpScope, bibleId))
				if script != dbpScript:
					updateRows.append(("script", script, dbpScript, bibleId))
				if copyright != dbpCopyright:
					if copyright != None:
						copyright = copyright.replace("'", "''")
					updateRows.append(("copyright", copyright, dbpCopyright, bibleId))

		tableName = "bibles"
		pkeyNames = ("id",)
		attrNames = ("language_id", "versification", "numeral_system_id", "date", "scope", "script", "copyright")
		ignoredNames = ("derived", "copyright", "priority", "reviewed", "notes") # here for doc only
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	def biblesLanguageId(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			iso = lptsRecord.ISO()
			iso = iso.lower() if iso != None else None
			langName = lptsRecord.LangName()
			langName = langName.lower() if langName != None else None
			result = self.languageMap1.get((iso, langName))
			#print("languageMap1", bibleId, iso, langName, result)
			if result != None:
				final.add(result)
		if len(final) == 0:
			for (lptsIndex, lptsRecord) in lptsRecords:
				iso = lptsRecord.ISO()
				iso = iso.lower() if iso != None else None
				langName = lptsRecord.LangName()
				langName = langName.lower() if langName != None else None
				result = self.languageMap2.get((iso, langName))
				#print("languageMap2", bibleId, iso, langName, result)
				if result != None:
					final.add(result)
		if len(final) == 0:
			for (lptsIndex, lptsRecord) in lptsRecords:
				iso = lptsRecord.ISO()
				iso = iso.lower() if iso != None else None
				result = self.languageMap3.get(iso)
				#print("languageMap3", bibleId, iso, result)
				if result != None:
					final.add(result)
		if len(final) == 0:
			print("ERROR_01 ISO code of bibleId unknown", iso, bibleId)
			return None
		if len(final) > 1:
			print("ERROR_02 Multiple language_id for bibleId", bibleId, final)
		return list(final)[0]


	def biblesScript(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			script = lptsRecord.Orthography(lptsIndex)
			if script != None:
				result = self.scriptNameMap.get(script)
				if result != None:
					final.add(result)
				else:
					print("ERROR_05 unknown script_id for name %s in bible_id %s" % (script, bibleId))
		if len(final) == 0:
			for (lptsIndex, lptsRecord) in lptsRecords:
				result = self.scriptCodeMap.get(lptsRecord.ISO())
				if result != None:
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


	def biblesNumeralId2(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			numerals = lptsRecord.Numerals()
			if numerals != None:
				numerals = numerals.strip().lower().replace(" ", "-")
				if numerals == "latin":
					numerals = "western-arabic"
				elif numerals == "arabic":
					numerals = "western-arabic"
				elif numerals == "arabic-/-khmer":
					numerals = "khmer"
				if numerals not in self.numeralIdSet:
					print("ERROR_03 Unknown Numeral System '%s' for %s" % (numerals, lptsRecord.Reg_StockNumber()))
				final.add(numerals)
		if len(final) == 0:
			return None
		if len(final) > 1:
			print("ERROR_04 Multiple Numerals for bibleId", bibleId, final)
		return list(final)[0]


	def biblesDate(self, copyright):
		if copyright != None:
			match = self.yearPattern.search(copyright)
			if match != None and match.group(1)[0] <= '2':
				return match.group(1)
		else:
			return None


	def biblesSizeCode(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			for typeCode in ["audio", "text", "video"]:
				damIds = lptsRecord.DamIdMap(typeCode, lptsIndex)
				for damId in damIds.keys():
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
				result = "NT"
			elif hasNTP:
				result = "NTP"
			if hasOT:
				result += "OT"
			elif hasOTP:
				result += "OTP"
			if result == "NTOT":
				return "C"
			return result


	def bibleTextCopyright(self, bibleId, lptsRecords):
		final = set()
		for (lptsIndex, lptsRecord) in lptsRecords:
			copyright = lptsRecord.Copyrightc()
			if copyright != None:
				if "©" in copyright:
					pos = copyright.index("©")
					copyright = copyright[pos:]
				copyright = copyright.split("\n")[0]
				copyright = copyright[:128]
				final.add(copyright)
		if len(final) == 0:
			return None
		elif len(final) == 1:
			return list(final)[0]
		else:
			year = 0
			result = ""
			for item in final:
				match = self.yearPattern.search(item)
				if match != None:
					matchInt = int(match.group(1))
					if matchInt > year:
						year = matchInt
						result = item
					elif matchInt == year:
						if len(item) > len(result):
							result = item
						elif len(item) == len(result) and item > result:
							result = item
			if year > 0:
				return result # return one with latest year, if years equal then longest, if same length then later
			else:
				result = ""
				for item in final:
					if len(item) > len(result):
						result = item
					elif len(item) == len(result) and item > result:
						result = item
				return result # return longest in order to get a consistent result


	def testBibleForeignKeys(self, bibleId):
		hasErrors = False
		hasErrors |= self.testDeleteInTable(bibleId, "user_bookmarks")
		hasErrors |= self.testDeleteInTable(bibleId, "user_highlights")
		hasErrors |= self.testDeleteInTable(bibleId, "user_notes")
		hasErrors |= self.testDeleteInTable(bibleId, "user_settings")
		return hasErrors


	def testDeleteInTable(self, bibleId, tableName):
		sql = ("SELECT count(*) FROM " +
				self.config.database_user_db_name +
				"." + tableName +
				" WHERE bible_id = %s")
		count = self.db.selectScalar(sql, (bibleId,))
		if count > 0:
			print("ERROR_06: Unable to DELETE bible_id %s from bibles. It is used by table %s." % (bibleId, tableName))
			return True
		else:
			return False # has no error


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
	#dbOut.execute("test-bibles")



