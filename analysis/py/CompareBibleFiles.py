# CompareBibleFiles.py




class CompareBibleFiles:

	def __init__(self, config):
		self.db = SQLUtility("localhost", 3306, "root", "valid_dbp")

	def compareCommon(self):
		#bucketErrors = []
		#typeCodeErrors = []
		#hashIdErrors = []
		#sizeCodeErrors = []
		#secondRowErrors = []
		sql = ("SELECT t.hash_id, t.book_id, t.chapter_start, t.verse_start,"
			" t.chapter_end, t.verse_end, t.file_name")
		sql = ("SELECT t.id, t.asset_id, t.set_type_code, t.set_size_code, t.hash_id,"
			" p.asset_id, p.set_type_code, p.set_size_code, p.hash_id"
			" FROM valid_dbp.bible_filesets t, dbp.bible_filesets p"
			" WHERE t.id = p.id"
			" AND p.set_type_code NOT IN ('text_plain', 'app')"
			" ORDER BY t.id, p.asset_id, p.set_type_code")

		[["hash_id", "book_id", "chapter_start", "verse_start"], 
			["chapter_end", "verse_end", "file_name", "file_size", "duration"]]
		resultSet = self.db.select(sql, ())
		filesetIdMap = {}
		for row in resultSet:
			filesetId = row[0]
			filesets = filesetIdMap.get(filesetId, [])
			filesets.append(row)
			filesetIdMap[filesetId] = filesets
		print(len(resultSet), "found")
		for filesetId, rows in filesetIdMap.items():
			row = rows[0]
			testBucket = row[1]
			testTypeCode = row[2]
			testSizeCode = row[3]
			testHashId = row[4]
			prodBucket = row[5]
			prodTypeCode = row[6]
			prodSizeCode = row[7]
			prodHashId = row[8]
			if testBucket != prodBucket:
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					bucketErrors.append(row)
			if testTypeCode != prodTypeCode:
				typeCodeErrors.append(row)
			if testHashId != prodHashId:
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					hashIdErrors.append(row)
			if testSizeCode != prodSizeCode:
				sizeCodeErrors.append(row)

			if len(rows) == 2:
				row = rows[1]
				testBucket = row[1]
				prodBucket = row[5]
				if testBucket != "dbp-prod" or prodBucket != "dbs-web":
					secondRowErrors.append(row)

			if len(rows) > 2:
				print("ERRORS")
				print(filesetId)
				for row in rows:
					print(row)
				sys.exit()

		self.reportErrors2("Bucket Errors", bucketErrors)
		self.reportErrors2("SetTypeCode Errors", typeCodeErrors)
		self.reportErrors2("HashId Errors", hashIdErrors)
		self.reportErrors2("SetSizeCode Errors", sizeCodeErrors)
		#self.reportErrors2("Second Row Errors", secondRowErrors) # same as setTypeCode Errors
		print("%d filesetid matches found" % (len(filesetIdMap.keys())))


	def reportErrors2(self, message, rows):
		print("%d %s" % (len(rows), message))
		for row in rows:
			if len(row) == 2:
				fileset = row[0]
				print("  ", row[1], '\t', fileset[0], fileset[1], fileset[2], fileset[3])
			else:
				#print(" ", row[0], row[1], row[2], row[3])
				print(row)
		print("")


config = Config("dev")
compare = CompareBibleFiles(config)
compare.compareCommon()