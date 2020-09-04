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

		for dbpBibleId in sorted(dbpBibleMap.keys()):
			if dbpBibleId not in lptsBibleMap.keys():
				deleteRows.append(dbpBibleId)

		for bibleId in sorted(lptsBibleMap.keys()):
			lptsRecords = lptsBibleMap[bibleId]
			languageId = self.biblesLanguageId(bibleId, lptsRecords)
			#versification = self.biblesVersification(bibleId, lptsRecords)
			script = self.biblesScript(bibleId, lptsRecords)
			numerals = self.biblesNumeralId2(bibleId, lptsRecords)
			date = self.biblesDate(bibleId, lptsRecords)
			scope = self.biblesSizeCode(bibleId, lptsRecords)
			copyright = self.bibleTextCopyright(bibleId, lptsRecords)

			if bibleId not in dbpBibleMap.keys():
				if languageId != None:
					insertRows.append((languageId, "protestant", numerals, date, scope, script, copyright, bibleId))
			else:
				(dbpLanguageId, dbpVersification, dbpNumerals, dbpDate, dbpScope, dbpScript, dbpCopyright) = dbpBibleMap[bibleId]
				if (languageId != dbpLanguageId or
					#versification != dbpVersification or
					numerals != dbpNumerals or
					date != dbpDate or
					scope != dbpScope or
					script != dbpScript or
					copyright != dbpCopyright):
					if languageId != None:
						updateRows.append((languageId, dbpVersification, numerals, date, scope, script, copyright, bibleId))

		#tableName = "bibles"
		tableName = "test_bibles"
		pkeyNames = ("id",)
		attrNames = ("language_id", "versification", "numeral_system_id", "date", "scope", "script", "copyright")
		ignoredNames = ("derived", "copyright", "priority", "reviewed", "notes") # here for doc only
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
			print("ERROR_01 ISO code of bibleId unknown", iso, bibleId)
			return None
		if len(final) > 1:
			print("ERROR_02 Multiple language_id for bibleId", bibleId, final)
		return list(final)[0]


#	def biblesVersification(self, bibleId, lptsRecords):
#		finalOT = set()
#		finalNT = set()
#		for (lptsIndex, lptsRecord) in lptsRecords:
#			if lptsRecord.OTOrder() != None:
#				finalOT.add(lptsRecord.OTOrder())
#			if lptsRecord.NTOrder() != None:
#				finalNT.add(lptsRecord.NTOrder())
#		if len(finalOT) > 1:
#			if "Traditional" in finalOT:
#				finalOT.remove("Traditional")
#			if len(finalOT) > 1:
#				print("ERROR_03 Multiple OTOrder for bibleId", bibleId, finalOT)
#		if len(finalNT) > 1:
#			if "Traditional" in finalNT:
#				finalNT.remove("Traditional")
#			if len(finalNT) > 1:
#				print("ERROR_04 Multiple NTOrder for bibleId", bibleId, finalNT)
#		otOrder = list(finalOT)[0] if len(finalOT) > 0 else ""
#		ntOrder = list(finalNT)[0] if len(finalNT) > 0 else ""
#		return otOrder + "," + ntOrder


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
					numerals = "eastern-arabic"
				elif numerals == "arabic-/-khmer":
					numerals = "khmer"
				if numerals not in self.numeralIdSet:
					print("ERROR_03 Unknown Numeral System '%s'" % numerals)
				final.add(numerals)
		if len(final) == 0:
			return None
		if len(final) > 1:
			print("ERROR_04 Multiple Numerals for bibleId", bibleId, final)
		return list(final)[0]


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
				copyright = copyright.replace("'", "''")
				final.add(copyright)
		if len(final) == 0:
			return None
		#elif len(final) > 1:
		#	print("WARN_07: Multiple Copyright for bibleId", bibleId, final)
		return list(final)[0]


	def extractYear(self, finalSet, value):
		if value != None:
			match = self.yearPattern.search(value)
			if match != None and match.group(1)[0] <= '2':
				finalSet.add(match.group(1))


	def findResult(self, bibleId, final, fieldName):
		if len(final) == 0:
			return None
		elif len(final) == 1:
			return list(final)[0]
		else:
			print("WARN_06: bible_id %s has multiple %s values |%s|" % (bibleId, fieldName, "|".join(final)))
			return list(final)[0]


	## This is a one-time function that creates a table of script codes.
	@staticmethod
	def lptsScriptCodesInsert(db):
		sql = ("CREATE TABLE IF NOT EXISTS lpts_script_codes ("
			" lpts_name VARCHAR(256) NOT NULL PRIMARY KEY,"
			" script_id CHAR(4) NOT NULL)")
			#, FOREIGN KEY (script_id) REFERENCES alphabets(script))")
		db.execute(sql, ())
		count = db.selectScalar("SELECT count(*) FROM lpts_script_codes", ())
		if count == 0:
			scriptCodes = {
				"Amharic":"Ethi", 
				"Arabic":"Arab", 
				"Armenian":"Armn",
				"Bengali":"Beng", 
				"Bengali Script":"Beng",
				"Berber":"Tfng",
				"Burmese":"Mymr", 
				"Canadian Aboriginal Syllabic":"Cans", 
				"Canadian Aboriginal Syllabics":"Cans", 
				"Cherokee Sylabary":"Cher", 
				"Cyrillic":"Cyrl", 
				"Devanagari":"Deva", 
				"Devangari":"Deva", 
				"Ethiopic":"Ethi", 
				"Ethoiopic":"Ethi", 
				"Ethopic":"Ethi", 
				"Ge'ez":"Ethi", 
				"Greek":"Grek", 
				"Gujarati":"Gujr", 
				"Gurmukhi":"Guru", 
				"Han":"Hani", 
				"Hangul (Korean)":"Kore", 
				"Hebrew":"Hebr", 
				"Japanese":"Jpan", 
				"Kannada":"Knda", 
				"Khmer":"Khmr", 
				"Khmer Script":"Khmr", 
				"Lao":"Laoo", 
				"Latin":"Latn", 
				"Latin (Africa)":"Latn", 
				"Latin (African)":"Latn", 
				"Latin (Latin America)":"Latn", 
				"Latin (Latin American)":"Latn", 
				"Latin (PNG)":"Latn", 
				"Latin (SE Asia)":"Latn", 
				"Malayalam":"Mlym", 
				"NA":"Zyyy", 
				"Oriya":"Orya",
				"Tamil":"Taml", 
				"Telugu":"Telu", 
				"Thaana":"Thaa",
				"Thai":"Thai", 
				"Tibetan":"Tibt"}
			insertRows = []
			for (key, value) in scriptCodes.items():
				insertRows.append((key, value))
			sql = "INSERT lpts_script_codes (lpts_name, script_id) VALUES (%s, %s)"
			db.executeBatch(sql, insertRows)


	## This is a one-time function that creates test_bibles table.
	@staticmethod
	def createTestBiblesTable(db):
		db.execute("DROP TABLE IF EXISTS test_bibles", ())
		db.execute("CREATE TABLE test_bibles SELECT * FROM bibles", ())
		db.execute("ALTER TABLE test_bibles MODIFY COLUMN scope VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL", ())
		db.execute("ALTER TABLE test_bibles MODIFY COLUMN script char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL", ())
		db.execute("ALTER TABLE test_bibles MODIFY COLUMN numeral_system_id VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL", ())
		db.execute("ALTER TABLE test_bibles add primary key (id)", ())
		db.execute("ALTER TABLE test_bibles add index (language_id)", ())
		db.execute("ALTER TABLE test_bibles add index (numeral_system_id)", ())
		db.execute("ALTER TABLE test_bibles add index (script)", ())
		db.execute("ALTER TABLE test_bibles add foreign key (script) references alphabets (script)", ())
		db.execute("ALTER TABLE test_bibles add foreign key (language_id) references languages (id)", ())
		db.execute("ALTER TABLE test_bibles add foreign key (numeral_system_id) references numeral_systems (id)", ())


## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	UpdateDBPBiblesTable.lptsScriptCodesInsert(db)
	UpdateDBPBiblesTable.createTestBiblesTable(db)
	dbOut = SQLBatchExec(config)
	lptsReader = LPTSExtractReader(config)
	bibles = UpdateDBPBiblesTable(config, db, dbOut, lptsReader)
	bibles.process()
	db.close()

	#dbOut.displayStatements()
	dbOut.displayCounts()
	dbOut.execute()



