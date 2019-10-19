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
		self.fileSeq = ""
		self.name = ""
		self.title = ""
		self.damid = ""
		self.unknown = []
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


	def setTitle(self, title):
		self.title = title


	def setDamid(self, damid):
		self.damid = damid


	def addUnknown(self, unknown):
		self.unknown.append(unknown)


	def getUnknown(self, index):
		return self.unknown[index] if index < len(self.unknown) else ""


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

		## {bookseq}___{chap}_{bookname}____{damid}.mp3 
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


	## {bookseq}_{bookname}_{chap}_{damid}.mp3
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


	## {bookseq}_{fileseq}__{bookname}_{chap}_____{damid}.mp3
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


 	## {fileseq}_{USFM}_{chap}_{versestart}-{verseend}_SET_{unknown}___{damid}.mp3
 	## audio/SNMNVS/SNMNVSP1DA16/052_GEN_027_18-29_Set_54____SNMNVSP1DA.mp3
	def audio4(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.-]+", filename)
		if len(parts) == 9:
			file.setFileSeq(parts[0])
			file.setBook(parts[1])
			file.setChap(parts[2])
			file.setVerseStart(parts[3])
			file.setVerseEnd(parts[4])
			file.addUnknown(parts[5])
			file.addUnknown(parts[6])
			file.setDamid(parts[7])
			file.setType(parts[8])
		else:
			file.errors.append("not 9 parts")
		filenameOut = file.fileSeq + file.book + file.chap + file.verseStart + file.verseEnd + file.getUnknown(0) + file.getUnknown(1) + file.damid + "." + file.type
		file.setFile(filename, filenameOut)
		return file


	## {lang}_{vers}_{bookseq}_{bookname}_{chap}_{versestart}-{verseend}_{unknown}_{unknown}.mp3
	## audio/SNMNVS/SNMNVSP1DA/SNM_NVS_01_Genesis_041_50-57_SET91_PASSAGE1.mp3
	def audio5(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.-]+", filename)
		if len(parts) == 10:
			file.addUnknown(parts[0])
			file.addUnknown(parts[1])
			file.setBookSeq(parts[2])
			file.setBookName(parts[3])
			file.setChap(parts[4])
			file.setVerseStart(parts[5])
			file.setVerseEnd(parts[6])
			file.addUnknown(parts[7])
			file.addUnknown(parts[8])
			file.setType(parts[9])
		else:
			file.errors.append("not 10 parts")
		filenameOut = file.getUnknown(0) + file.getUnknown(1) + file.bookSeq + file.name + file.chap + file.verseStart + file.verseEnd + file.getUnknown(2) + file.getUnknown(3) + "." + file.type
		file.setFile(filename, filenameOut)
		return file		


	## {bookseq}___{fileseq}_{bookname}_{chap}_{startverse}_{endverse}{name}__damid.mp3
	## audio/PRSGNN/PRSGNNS1DA/B01___22_Genesis_21_1_10BirthofIsaac__S1PRSGNN.mp3
	def audio6(self, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.]+", filename)
		if len(parts) > 7:
			file.setBookSeq(parts[0])
			file.setFileSeq(parts[1])
			file.setBookName(parts[2])
			file.setChap(parts[3])
			file.setVerseStart(parts[4])
			(verseEnd, title) = self.splitNumAlpha(parts[5])
			file.setVerseEnd(verseEnd)
			parts[5] = verseEnd
			if title != None:
				parts.insert(6, title)
				self.combineName(parts, 6, 9)
				file.setTitle(parts[6])
			else:
				self.combineName(parts, 5, 8)
				file.setTitle(parts[6])
			file.setDamid(parts[-2])
			file.setType(parts[-1])
		else:
			file.errors.append("not 8 parts")
		filenameOut = file.bookSeq + file.fileSeq + file.name + file.chap + file.verseStart + file.verseEnd + file.title + file.damid + "." + file.type
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


	def splitNumAlpha(self, string):
		for index in range(len(string)):
			if string[index].isalpha():
				return (string[:index], string[index:])
		return (string, None)



	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s limit 10000000000")
		sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s AND bible_id='PRSGNN'"))
		filenamesMap = db.selectMapList(sql, (typeCode))
		db.close()
		self.parsers = [self.audio1, self.audio2, self.audio3, self.audio4, self.audio5, self.audio6]

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









