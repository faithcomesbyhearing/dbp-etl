# UpdateDBPBibleBooksTable.py


import csv
from Config import *
from SQLUtility import *
from SQLBatchExec import *


class UpdateBibleBooksTable:


	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db
		self.dbOut = dbOut
		self.bookNameMap = self.db.selectMap("SELECT id, name FROM books", ())


	## Update table with bible books data from audio or video fileset
	def createIfAbsent(self, filesetPrefix):
		(typeCode, bibleId, filesetId) = filesetPrefix.split("/")
		if typeCode in {"audio", "video"}:
			bookCount = db.selectScalar("SELECT count(*) FROM bible_books WHERE bible_id = %s", (bibleId,))
			if bookCount == 0:
				bookIdList = []
				bookChapterMap = {}
				priorBookId = None
				priorChapter = None
				csvFilename = self.config.directory_accepted + filesetPrefix.replace("/", "_") + ".csv"
				with open(csvFilename, newline='\n') as csvfile:
					reader = csv.DictReader(csvfile)
					for row in reader:
						bookId = row["book_id"]
						chapter = row["chapter_start"]
						if bookId != priorBookId:
							bookIdList.append(bookId)
							bookChapterMap[bookId] = [chapter]
						elif chapter != priorChapter:
							chapters = bookChapterMap[bookId]
							chapters.append("," + chapter)
							bookChapterMap[bookId] = chapters
						priorBookId = bookId
						priorChapter = chapter

				insertRows = []
				for bookId in bookIdList:
					bookName = self.bookNameMap.get(bookId)
					chapters = bookChapterMap[bookId]
					insertRows.append((bookName, bookName, chapters, bibleId, bookId))

				tableName = "bible_books"
				pkeyNames = ("bible_id", "book_id")
				attrNames = ("name", "name_short", "chapters")
				self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)


	## Update table with bible books data from alethia
	def process(self):
		print("TBD")



if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	books = UpdateBibleBooksTable(config, db, dbOut)
	books.createIfAbsent("audio/ENGESV/ENGESVN2DA")
	#books.process()
	dbOut.displayStatements()
	dbOut.displayCounts()
	#dbOut.execute()