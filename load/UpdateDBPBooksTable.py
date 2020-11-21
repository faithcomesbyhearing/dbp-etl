# UpdateDBPBooks Table
#
# This class is used to update the bible_books table from audio, text, and video sources
# However, this data is not updated for each fileset for each bibleId
# So we use the most complete fileset to update the sequence.
#
# Assign a sequence number for each book as it will appear in bible_books
#
# 1) For audio, use the Ann,Bnn sequence that was in the filename
# 2) For video, assign the Bnn sequence number based on book_id
# 3) Text files often have a different numeric sequence, which will make it difficult to merge disjoint filesets.  
# Ergo, normalize the text sequence to the Ann, Bnn form as follows:  
# Lookup the Ann sequence code of the first OT book and Bnn sequence number of the first NT book, 
# and then assign all the remaining books with a sequentally assigned Ann, Bnn code based upon 
# the sequence found in the metadata.xml file.
#
# Compare the incoming data with what is in bible_books right now.
#
# If the new book_id is not in bible_books insert the whole new record
# If the book_id is in book_id update as follows:
#   if the new source is text, update the book name, name_short, if different
#	if the new source is audio, update the sequence, if different
#
# If any sequence numbers have been duplicated within a bible_id, report it as an error
#
# In order to find records in bible_books that need to be deleted, 
# select all distinct book_id from all fileset in the bible 
# and delete from bible_books any that are missing from that select

from Config import *
from SqliteUtility import *
from SQLUtility import *
from SQLBatchExec import *


class TOCBook:

	def __init__(self, bookId, bookSeq, name, nameShort, chapters):
		self.bookId = bookId
		self.bookSeq = bookSeq
		self.name = name
		self.nameShort = nameShort
		self.chapters = chapters


class UpdateDBPBooksTable:

	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		#def __init__(self, config, typeCode, bibleId, filesetId):
		#self.config = config
		##self.typeCode = typeCode
		##elf.bibleId = bibleId
		##self.filesetId = filesetId
		##self.tocBooks = []
		self.extraBooks = {
			"FRT": "001", # Front Matter
			"INT": "002", # Introductions
			"BAK": "X01", # Back Matter
			"CNC": "X02", # Concordance
			"GLO": "X03", # Glossary
			"TDX": "X04", # Topical Index
			"NDX": "X05", # Names Index
			"OTH": "X06", # Other
			"XXA": "XXA",
			"XXB": "XXB",
			"XXC": "XXC",
			"XXD": "XXD",
			"XXE": "XXE",
			"XXF": "XXF",
			"XXG": "XXG"}
		self.otBooks = {			
			"GEN": "A01", # Genesis
			"EXO": "A02", # Exodus
			"LEV": "A03", # Leviticus
			"NUM": "A04", # Numbers
			"DEU": "A05", # Deuteronomy
			"JOS": "A06", # Joshua
			"JDG": "A07", # Judges
			"RUT": "A08", # Ruth
			"1SA": "A09", # 1 Samuel
			"2SA": "A10", # 2 Samuel
			"1KI": "A11", # 1 Kings
			"2KI": "A12", # 2 Kings
			"1CH": "A13", # 1 Chronicles
			"2CH": "A14", # 2 Chronicles
			"EZR": "A15", # Ezra
			"NEH": "A16", # Nehemiah
			"EST": "A17", # Esther (Hebrew)
			"JOB": "A18", # Job
			"PSA": "A19", # Psalms
			"PRO": "A20", # Proverbs
			"ECC": "A21", # Ecclesiastes
			"SNG": "A22", # Song of Songs
			"ISA": "A23", # Isaiah
			"JER": "A24", # Jeremiah
			"LAM": "A25", # Lamentations
			"EZK": "A26", # Ezekiel
			"DAN": "A27", # Daniel (Hebrew)
			"HOS": "A28", # Hosea
			"JOL": "A29", # Joel
			"AMO": "A30", # Amos
			"OBA": "A31", # Obadiah
			"JON": "A32", # Jonah
			"MIC": "A33", # Micah
			"NAM": "A34", # Nahum
			"HAB": "A35", # Habakkuk
			"ZEP": "A36", # Zephaniah
			"HAG": "A37", # Haggai
			"ZEC": "A38", # Zechariah
			"MAL": "A39", # Malachi
			"TOB": "A68", # Tobit
			"JDT": "A69", # Judith
			"ESG": "A70", # Esther Greek
			"WIS": "A71", # Wisdom of Solomon 
			"SIR": "A72", # Sirach
			"BAR": "A73", # Baruch
			"LJE": "A74", # Letter of Jeremiah
			"S3Y": "A75", # Song of the 3 Young Men
			"SUS": "A76", # Susanna
			"BEL": "A77", # Bel and the Dragon
			"1MA": "A78", # 1 Maccabees
			"2MA": "A79", # 2 Maccabees
			"3MA": "A80", # 3 Maccabees
			"4MA": "A81", # 4 Maccabees
			"1ES": "A82", # 1 Esdras (Greek)
			"2ES": "A83", # 2 Esdras (Latin)
			"MAN": "A84", # Prayer of Manasseh
			"PS2": "A85", # Psalm 151
			"ODA": "A86", # Odae/Odes
			"PSS": "A87", # Psalms of Solomon
			"EZA": "C04", # Ezra Apocalypse
			"5EZ": "C05", # 5 Ezra
			"6EZ": "C06", # 6 Ezra
			"DAG": "D02", # Daniel Greek
			"PS3": "D03", # Psalms 152-155
			"2BA": "D04", # 2 Baruch
			"LBA": "D05", # Letter of Baruch
			"JUB": "D06", # Jubilees
			"ENO": "D07", # Enoch
			"1MQ": "D08", # 1 Meqabyan/Mekabis
			"2MQ": "D09", # 2 Meqabyan/Mekabis
			"3MQ": "E00", # 3 Meqabyan/Mekabis
			"REP": "E01", # Reproof
			"4BA": "E02", # 4 Baruch
			"LAO": "E03"} # Letter to Laodiceans
		self.ntBooks = {			
			"MAT": "B01", # Matthew
			"MRK": "B02", # Mark
			"LUK": "B03", # Luke
			"JHN": "B04", # John
			"ACT": "B05", # Acts
			"ROM": "B06", # Romans
			"1CO": "B07", # 1 Corinthians
			"2CO": "B08", # 2 Corinthians
			"GAL": "B09", # Galatians
			"EPH": "B10", # Ephesians
			"PHP": "B11", # Philippians
			"COL": "B12", # Colossians
			"1TH": "B13", # 1 Thessalonians
			"2TH": "B14", # 2 Thessalonians
			"1TI": "B15", # 1 Timothy
			"2TI": "B16", # 2 timothy
			"TIT": "B17", # Titus
			"PHM": "B18", # Philemon
			"HEB": "B19", # Hebrews
			"JAS": "B20", # James
			"1PE": "B21", # 1 Peter
			"2PE": "B22", # 2 Peter
			"1JN": "B23", # 1 John
			"2JN": "B24", # 2 John
			"3JN": "B25", # 3 John
			"JUD": "B26", # Jude
			"REV": "B27"} # Revelation


	## temporary method to add the book_seq column to the bible_books table
	def alterBibleBooksTable(self):
		sql = ("SELECT count(*) FROM information_schema.columns"
		" WHERE table_name = 'bible_books' AND table_schema = %s AND column_name = 'book_seq'")
		count = self.db.selectScalar(sql, (self.config.database_db_name,))
		if count == 0:
			self.db.execute("ALTER TABLE bible_books ADD COLUMN book_seq char(4) NULL", ())


	## extract a book level table of contents for this fileset we are updating in DBP
	def getTableOfContents(self, typeCode, bibleId, filesetId):
		tocBooks = []
		if typeCode == "text":
			firstOTBook = True
			firstNTBook = True
			extraBook = True
			priorBookSeq = None
			bibleDB = SqliteUtility(self.config.directory_accepted + filesetId + ".db")
			resultSet = bibleDB.select("SELECT code, title, name, chapters FROM tableContents ORDER BY rowId", ())
			for (bookId, title, name, chapters) in resultSet:
				if firstOTBook and bookId in self.otBooks.keys():
					firstOTBook = False
					bookSeq = self.otBooks[bookId]
				elif firstNTBook and bookId in self.ntBooks.keys():
					firstNTBook = False
					bookSeq = self.ntBooks[bookId]
				elif extraBook and bookId in self.extraBooks.keys():
					extraBook = False
					bookSeq = self.extraBooks[bookId]
				else:
					prefix = priorBookSeq[0]
					number = int(priorBookSeq[1:]) + 1
					bookSeq = prefix + str(number) if number > 9 else prefix + "0" + str(number)
				priorBookSeq = bookSeq
				tocBook = TOCBook(bookId, bookSeq, title, name, chapters)
				tocBooks.append(tocBook)
			bibleDB.close()

		elif typeCode in {"audio", "video"}:
			bookChapterMap = {}
			priorBookId = None
			priorChapter = None
			csvFilename = self.config.directory_accepted + "%s_%s_%s.csv" % (typeCode, bibleId, filesetId)
			with open(csvFilename, newline='\n') as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					bookId = row["book_id"]
					bookSeq = row["sequence"]
					bookName = row["book_name"]
					chapter = row["chapter_start"]
					if bookId != priorBookId:
						tocBook = TOCBook(bookId, bookSeq, bookName, bookName, None)
						tocBooks.append(tocBook)
						bookChapterMap[bookId] = [chapter]
					elif chapter != priorChapter:
						chapters = bookChapterMap[bookId]
						chapters.append("," + chapter)
						bookChapterMap[bookId] = chapters
					priorBookId = bookId
					priorChapter = chapter

			for tocBook in this.tocBooks:
				tocBook.chapters = bookChapterMap[tocBook.bookId]

			if typeCode == "video":
				seqMap = {"MAT": "B01", "MRK": "B02", "LUK": "B03", "JHN": "B04", "ACT": "B05"}
				nameMap = {"MAT": "Matthew", "MRK": "Mark", "LUK": "Luke", "JHN": "John", "ACT": "Acts"}
				for tocBook in this.tocBooks:
					tocBook.bookSeq = seqMap.get(tocBook.bookId)
					tocBook.name = nameMap.get(tocBook.bookId)
					tocBook.nameShort = tocBook.name
		return tocBooks


	## extract sequence of the current bible books table, and merge to tocBooks
	def updateBibleBooks(self, typeCode, bibleId, tocBooks):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = "SELECT book_id, book_seq, name, name_short, chapters FROM bible_books WHERE bible_id = %s ORDER BY book_seq"
		resultSet = db.select(sql, (bibleId,))
		bibleBookMap = {}
		for row in resultSet:
			bibleBookMap[row[0]] = row

		for toc in tocBooks:
			if toc.bookId not in bibleBookMap.keys():
				insertRows.append((toc.bookSeq, toc.name, toc.nameShort, toc.chapters, bibleId, toc.bookId))

			elif typeCode == "text":
				(dbpBookId, dbpBookSeq, dbpName, dbpNameShort, dbpChapters) = bibleBookMap[toc.bookId]
				if toc.name != dbpName:
					updateRows.append(("name", toc.name, dbpName, bibleId, toc.bookId))
				if toc.nameShort != dbpNameShort:
					updateRows.append(("nameShort", toc.nameShort, dbpNameShort, bibleId, toc.bookId))

			elif typeCode == "audio":
				(dbpBookId, dbpBookSeq, dbpName, dbpNameShort, dbpChapters) = bibleBookMap[toc.bookId]	
				if toc.bookSeq != dbpBookSeq:
					updateRows.append(("book_seq", toc.bookSeq, dbpBookSeq, bibleId, toc.bookId))

		sql = ("SELECT distinct book_id FROM bible_files bf"
			" JOIN bible_fileset_connections bfc ON bf.hash_id = bfc.hash_id"
			" WHERE bfc.bible_id = %s")
		dbpBookIdSet = self.db.selectSet(sql, (bibleId,))
		for toc in tocBooks:
			dbpBookIdSet.add(toc.bookId)
		for bookId in bibleBookMap.keys():
			if bookId not in dbpBookIdSet:
				deleteRows.append((bibleId, bookId))

		tableName = "bible_books"
		pkeyNames = ("bible_id", "book_id")
		attrNames = ("book_seq", "name", "name_short", "chapters")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)

		# Perform this query to look for duplication.
		# SELECT book_id, book_seq, name, name_short, chapters FROM bible_books WHERE bible_id = %s AND book_seq IN
		# (SELECT book_seq FROM bible_books WHERE bible_id = %s GROUP BY book_seq HAVING count(*) > 1)


	def updateEntireBibleBooksTable():
		print("TBD")
		# Doing this would require reading all records in bible_files
		# Putting them into a map by bibleId, in order to do one bible_id at a time.
		# Parsing the filenames, hopefully with FilenameParser or doing a simplified parse
		# The parse only needs to get typeCode, book_id, book_seq, name, chapters
		# Then use the above information to build TOCBook objects
		# Then process each TOCBook



## Unit Test
if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPBooksTable(config, db, dbOut)
	testCases = [("text", "AA1WBT", "AA1WBT"),
				("text", "ENGWEBT", "ENGWEBT"),
				("text", "PESNMV", "PESNMV")]
	for (typeCode, bibleId, filesetId) in testCases:
		update.alterBibleBooksTable()
		tocBooks = update.getTableOfContents(typeCode, bibleId, filesetId)
		update.updateBibleBooks(typeCode, bibleId, tocBooks)

	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute("test-books")


