# FilenameParser.py

import io
import sys
import re
from Lookup import *
from SQLUtility import *

class Filename:

	def __init__(self, chapterMap):
		self.chapterMap = chapterMap
		self.book = ""
		self.chap = ""
		self.verseStart = ""
		self.verseEnd = ""
		self.bookSeq = ""
		self.fileSeq = "" # used by a minority
		self.name = ""
		self.damid = ""
		self.unknown1 = ""
		self.unknown2 = ""
		self.type = ""
		self.file = ""
		self.errors = []


	def setBookSeq(self, bookSeq):
		self.bookSeq = bookSeq
		if not bookSeq[1:].isdigit():
			self.errors.append("non-number bookSeq")


	def setFileSeq(self, fileSeq):
		self.fileSeq = fileSeq
		if not fileSeq.isdigit():
			self.errors.append("non-number fileSeq")


	# Should be used before set chapter
	def setBookName(self, name):
		self.name = name
		bookId = Lookup().usfmBookId(name)
		if bookId == None:
			self.errors.append("usfm not found for name")
		else:
			self.setBook(bookId)


	def setBook(self, bookId):
		self.book = bookId
		if bookId not in self.chapterMap.keys():
			self.errors.append("usfm code is not valid")


	def setChap(self, chap):
		self.chap = chap
		if not chap.isdigit():
			self.errors.append("non-number chap")
		elif self.book != None:
			chapter = self.chapterMap.get(self.book)
			if chapter != None and int(chap) > int(chapter):
				self.errors.append("chap too large")


	def setVerseStart(self, verseStart):
		self.verseStart = verseStart
		if not verseStart.isdigit():
			self.errors.append("non-number verse start")


	def setVerseEnd(self, verseEnd):
		self.verseEnd = verseEnd
		if not verseEnd.isdigit():
			self.errors.append("non-number verse end")


	def setDamid(self, damid):
		self.damid = damid


	def setUnknown1(self, unknown):
		self.unknown1 = unknown


	def setUnknown2(self, unknown):
		self.unknown2 = unknown


	def setType(self, typ):
		self.type = typ


	def setFile(self, filename, genFilename):
		self.file = filename
		if genFilename.replace("_", "") != filename.replace("_","").replace("-",""):
			self.errors.append("Mismatch %s" % (genFilename))


	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.errors)


class FilenameParser:

	def __init__(self):
		self.parsedList = []
		self.unparsedList = []

		## {book_seq}___{chap}_{bookname}____{damid}.mp3 
		## B01___01_Matthew_____ENGGIDN2DA.mp3
	def audio1(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.]+", filename)
		file.setType(parts[-1])
		# split name from damid if there is no _ between
		if any(c.islower() for c in parts[-2]):
			newParts = self.splitDamId(parts[-2])
			parts[-2] = newParts[1]
			parts.insert(-2, newParts[0])
		file.setDamid(parts[-2])
		if len(parts) > 4:
			self.combineName(parts, 2, 5)
			file.setBookSeq(parts[0])
			file.setBookName(parts[2])
			file.setChap(parts[1])
		else:
			file.errors.append("less than 5 parts")
		filenameOut = file.bookSeq + file.chap + file.name + file.damid + "." + file.type
		file.setFile(filename, filenameOut)
		return file


	## {book_seq}_{bookname}_{chap}_{damid}.mp3
	## B01_Genesis_01_S1COXWBT.mp3
	def audio2(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.]+", filename)
		if len(parts) > 4:
			self.combineName(parts, 1, 5)
			file.setBookSeq(parts[0])
			file.setBookName(parts[1])
			file.setChap(parts[2])
			file.setDamid(parts[3])
			file.setType(parts[4])
		else:
			file.errors.append("not 5 parts")
		filenameOut = file.bookSeq + file.name + file.chap + file.damid + "." + file.type
		file.setFile(filename, filenameOut)
		return file


	## {book_seq}_{file_seq}__{bookname}_{chap}_____{damid}.mp3
	## A08_073__Ruth_01_________S2RAMTBL.mp3
	def audio3(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.]+", filename)
		if len(parts) > 5:
			self.combineName(parts, 2, 6)
			file.setBookSeq(parts[0])
			file.setFileSeq(parts[1])
			file.setBookName(parts[2])
			file.setChap(parts[3])
			file.setDamid(parts[4])
			file.setType(parts[5])
		else:
			file.errors.append("not 6 parts")
		filenameOut = file.bookSeq + file.fileSeq + file.name + file.chap + file.damid + "." + file.type
		file.setFile(filename, filenameOut)
		return file


 	## {file_seq}_{USFM}_{chap}_{verse_start}-{verse_end}_SET_{unknown}___{damid}.mp3
 	## 052_GEN_027_18-29_Set_54____SNMNVSP1DA.mp3
	def audio4(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.-]+", filename)
		if len(parts) == 9:
			file.setFileSeq(parts[0])
			file.setBook(parts[1])
			file.setChap(parts[2])
			file.setVerseStart(parts[3])
			file.setVerseEnd(parts[4])
			file.setUnknown1(parts[5])
			file.setUnknown2(parts[6])
			file.setDamid(parts[7])
			file.setType(parts[8])
		else:
			file.errors.append("not 9 parts")
		filenameOut = file.fileSeq + file.book + file.chap + file.verseStart + file.verseEnd + file.unknown1 + file.unknown2 + file.damid + "." + file.type
		file.setFile(filename, filenameOut)
		return file


	def splitDamId(self, string):
		for index in range(len(string) -1, 0, -1):
			if string[index].islower():
				return (string[:index + 1], string[index + 1:])


	def combineName(self, parts, namePart, maxParts):
		while (len(parts) > maxParts):
			parts[namePart] = parts[namePart] + "_" + parts[namePart + 1]
			parts.pop(namePart + 1)



	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s limit 10000000000")
		sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s AND bible_id='SNMNVS'"))
		filenamesMap = db.selectMapList(sql, (typeCode))
		db.close()
		self.parsers = [self.audio1, self.audio2, self.audio3, self.audio4]

		for prefix in filenamesMap.keys():
			filenames = filenamesMap[prefix]
			(numErrors, parser, files) = self.parseFileset(prefix, filenames)
			parserName = parser.__name__
			if numErrors == 0:
				self.parsedList.append((parserName, prefix))
			else:
				self.unparsedList.append((numErrors, parserName, prefix))
				for file in files:
					if len(file.errors) > 0:
						print(parserName, prefix, file.file, ", ".join(file.errors))


	def parseFileset(self, prefix, filenames):
		parserTries = []
		for parser in self.parsers:
			(numErrors, parser, files) = self.parseOneFileset(parser, prefix, filenames)
			if numErrors == 0:
				return (numErrors, parser, files)
			parserTries.append((numErrors, parser, files))
		best = 1000000
		selected = None
		for aTry in parserTries:
			if aTry[0] < best:
				best = aTry[0]
				selected = aTry
		return selected


	def parseOneFileset(self, parser, prefix, filenames):
		numErrors = 0
		files = []
		for filename in filenames:
			file = parser(filename)
			files.append(file)
			if len(file.errors) > 0:
				numErrors += 1
				#print(parser.__name__, prefix, filename, ", ".join(file.errors))
		return (numErrors, parser, files)


	def summary(self):
		file = io.open("FilenameParser.out", mode="w", encoding="utf-8")
		for entry in self.parsedList:
			file.write("%s  %s\n" % entry)
		file.write("\n\nUnparsed\n\n")
		for entry in self.unparsedList:
			file.write("%d  %s  %s\n" % entry)
		file.close()


parser = FilenameParser()
parser.process()
parser.summary()









