# UpdateDBPBooks Table
#

"""
On a text load, pull the data from the sqlite bible.

Also, pull dbp 

1) When doing a text upload, pull the TOC from sqlite, and put data into a standard form.

2) When doing an audio or video upload, pull data from csvFile of parsed fileset and put data into a standard form

3) Make certain that video data is in MAT, MRK, LUK, JHN sequence, and put into standard TOC form

4) Put all of these into a std form. I think this should be a and array of TOCBook class

5) Pull data from all other text and audio and video from DBP.bible_files, 

6) Exclude the fileset being loaded from DBP select

7) Put this sequence of books into an ordered list of bookId

8) Merge the data, show preference for video, or the fileset with the most

9) When there are differences in sequence show an error

10) Take result and the new data, and perform std update logic with contents of bible_books

What is the logic for merging the data.

0) We form a set that is the union of all books from all source.

1) We start with the sequence from Audio, is a complete set we select audio
2) We move to text, if that is a complete set we select text
3) We move to video, if that is a complete set we select video
4) If there are gaps, we use a default sequence from books.
"""

class TOCBook:

	def __init__(self, bookId, name, nameShort, chapters):
		this.bookId = bookId
		this.name = name
		this.nameShort = nameShort
		this.chapters = chapters


class UpdateDBPBooksTable:


	def updateBibleBooks(self, bibleDB, bibleId):
		insertRows = []
		updateRows = []
		deleteRows = []
		sql = "SELECT book_id, name, name_short, chapters FROM bible_books WHERE bible_id = %s"
		resultSet = self.db.select(sql, (bibleId,))
		dbpBooksMap = {}
		for (dbpBookId, dbpName, dbpNameShort, dbpChapters) in resultSet:
			dbpBooksMap[dbpBookId] = (dbpName, dbpNameShort, dbpChapters)

		ssBookIdSet = set()
		for (ssBookId, ssTitle, ssName, ssChapterList) in bibleDB.select("SELECT code, title, name, chapters FROM tableContents", ()):
			ssBookIdSet.add(ssBookId)

			if ssBookId not in dbpBooksMap.keys():
				insertRows.append((ssTitle, ssName, ssChapterList, bibleId, ssBookId))
			else:
				(dbpName, dbpNameShort, dbpChapters) = dbpBooksMap[ssBookId]
				if dbpName != ssTitle:
					updateRows.append(("name", ssTitle, dbpName, bibleId, ssBookId))
				if dbpNameShort != ssName:
					updateRows.append(("name_short", ssName, dbpNameShort, bibleId, ssBookId))
				if dbpChapters != ssChapterList:
					updateRows.append(("chapters", ssChapterList, dbpChapters, bibleId, ssBookId))

		for dbpBookId in dbpBooksMap.keys():
			if dbpBookId not in ssBookIdSet:
				deleteRows.append((bibleId, dbpBookId))

		tableName = "bible_books"
		pkeyNames = ("bible_id", "book_id")
		attrNames = ("name", "name_short", "chapters")
		self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
		self.dbOut.updateCol(tableName, pkeyNames, updateRows)
		self.dbOut.delete(tableName, pkeyNames, deleteRows)


	## Update table with bible books data from audio or video fileset
	def insertBibleBooksIfAbsent(self, filesetPrefix):
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




