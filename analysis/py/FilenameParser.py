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


	def setFile(self, template, filename):
		self.file = filename
		fileOut = []
		miscIndex = 0
		for item in template.parts:
			if item == "book_seq":
				fileOut.append(self.bookSeq)
			elif item == "file_seq":
				fileOut.append(self.fileSeq)
			elif item == "book_name":
				fileOut.append(self.name.replace("_",""))
			elif item == "book_id":
				fileOut.append(self.book)
			elif item == "chapter":
				fileOut.append(self.chap)
			elif item == "verse_start":
				fileOut.append(self.verseStart)
			elif item == "verse_end":
				fileOut.append(self.verseEnd)
			elif item == "title":
				fileOut.append(self.title.replace("_",""))
			elif item == "damid":
				fileOut.append(self.damid)
			elif item == "type":
				fileOut.append(self.type)
			elif item == "misc":
				fileOut.append(self.getUnknown(miscIndex))
				miscIndex += 1
		filenameOut = "".join(fileOut)
		if filenameOut != filename.replace("_","").replace("-","").replace(".",""):
			self.errors.append("Mismatch %s" % (filenameOut))


	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.errors)


class FilenameTemplate:

	def __init__(self, name, parts, specialInst):
		self.name = name
		self.parts = parts
		self.numParts = len(parts)
		self.namePosition = None
		for index in range(len(parts)):
			part = parts[index]
			if part not in {"book_id", "chapter", "verse_start", "verse_end", 
				"book_seq", "file_seq", "book_name", "title", "damid", "type", "misc"}:
				print("ERROR: filenameTemplate part %s is not known" % (part))
				sys.exit()
			if part in {"book_name", "title"}:
				self.namePosition = index
		self.hasProblemDamId = ("damid_front_clean" in specialInst)
		self.verseEndClean = ("verse_end_clean" in specialInst)


class FilenameParser:

	def __init__(self):
		self.parsedList = []
		self.unparsedList = []
		self.audioTemplates = (
			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_Matthew_____ENGGIDN2DA.mp3
			FilenameTemplate("audio1", ("book_seq", "chapter", "book_name", "damid", "type"), ("damid_front_clean",)),
			## {bookseq}_{bookname}_{chap}_{damid}.mp3   B01_Genesis_01_S1COXWBT.mp3
			FilenameTemplate("audio2", ("book_seq", "book_name", "chapter", "damid", "type"), ()),
			## {bookseq}_{fileseq}__{bookname}_{chap}_____{damid}.mp3   A08_073__Ruth_01_________S2RAMTBL.mp3
			FilenameTemplate("audio3", ("book_seq", "file_seq", "book_name", "chapter", "damid", "type"), ()),
 			## {fileseq}_{USFM}_{chap}_{versestart}-{verseend}_SET_{unknown}___{damid}.mp3   audio/SNMNVS/SNMNVSP1DA16/052_GEN_027_18-29_Set_54____SNMNVSP1DA.mp3
			FilenameTemplate("audio4", ("file_seq", "book_id", "chapter", "verse_start", "verse_end", "misc", "misc", "damid", "type"), ()),
			## {lang}_{vers}_{bookseq}_{bookname}_{chap}_{versestart}-{verseend}_{unknown}_{unknown}.mp3  audio/SNMNVS/SNMNVSP1DA/SNM_NVS_01_Genesis_041_50-57_SET91_PASSAGE1.mp3
			FilenameTemplate("audio5", ("misc", "misc", "book_seq", "book_name", "chapter", "verse_start", "verse_end", "misc", "misc", "type"), ()),
			## {bookseq}___{fileseq}_{bookname}_{chap}_{startverse}_{endverse}{name}__damid.mp3   audio/PRSGNN/PRSGNNS1DA/B01___22_Genesis_21_1_10BirthofIsaac__S1PRSGNN.mp3
			FilenameTemplate("audio6", ("book_seq", "file_seq", "book_name", "chapter", "verse_start", "verse_end", "title", "damid", "type"), ("verse_end_clean",)),
			## {misc}_{misc}_Set_{fileseq}_{bookname}_{chap}_{verse_start}-{verse_end}.mp3   Nikaraj_P2KFTNIE_Set_051_Luke_21_1-19.mp3
			FilenameTemplate("audio7", ("misc", "misc", "misc", "file_seq", "book_name", "chapter", "verse_start", "verse_end", "type"), ()),
			## {file_seq}_{testament}_{KDZ}_{vers}_{bookname}_{chap}.mp3   1215_O2_KDZ_ESV_PSALM_57.mp3
			FilenameTemplate("audio8", ("file_seq", "misc", "misc", "misc", "book_name", "chapter", "type"), ()),
			## {bookseq}__{fileseq}_{non-standar-book-id}_{chapter}_{chapter_end}_{damid}.mp3   A01__002_GEN_1_2__S1DESWBT.mp
			FilenameTemplate("audio9", ("book_seq", "file_seq", "book_name", "chapter", "misc", "damid", "type"), ()),
			## {fileseq}_{title}.mp3
			##FilenameTemplate("audio10", ("file_seq", "title", "type"), ())
		)


	def parse(self, template, filename):
		file = Filename(self.chapterMap)
		parts = re.split("[_.-]+", filename)
		if template.hasProblemDamId:
			self.splitDamIdIfNeeded(parts, -2)
		if template.verseEndClean:
			verseEndPos = template.parts.index("verse_end")
			if verseEndPos < len(parts):
				self.splitNumAlpha(parts, verseEndPos)
		if len(parts) > template.numParts and template.namePosition != None:
			self.combineName(parts, template.namePosition, template.numParts)
		if template.numParts == len(parts):
			for index in range(template.numParts):
				part = parts[index]
				item = template.parts[index]
				if item == "book_seq":
					file.setBookSeq(part)
				elif item == "file_seq":
					file.setFileSeq(part)
				elif item == "book_name":
					file.setBookName(part)
				elif item == "book_id":
					file.setBook(part)
				elif item == "chapter":
					file.setChap(part)
				elif item == "verse_start":
					file.setVerseStart(part)
				elif item == "verse_end":
					file.setVerseEnd(part)
				elif item == "title":
					file.setTitle(part)
				elif item == "damid":
					file.setDamid(part)
				elif item == "type":
					file.setType(part)
				elif item == "misc":
					file.addUnknown(part)
				else:
					print("ERROR: unknown type in template %s" % (item))
					sys.exit()	
		else:
			file.errors.append(("expect %d parts, have %d" % (template.numParts, len(parts))))
		file.setFile(template, filename)
		return file


	def splitDamIdIfNeeded(self, parts, damidIndex):
		damid = parts[damidIndex]
		if any(c.islower() for c in damid):
			for index in range(len(damid) -1, 0, -1):
				if damid[index].islower():
					parts[damidIndex] = damid[index + 1:]
					parts.insert(damidIndex, damid[:index + 1])
					break


	def combineName(self, parts, namePart, maxParts):
		while (len(parts) > maxParts):
			parts[namePart] = parts[namePart] + "_" + parts[namePart + 1]
			parts.pop(namePart + 1)


	def splitNumAlpha(self, parts, splitPart):
		string = parts[splitPart]
		for index in range(len(string)):
			if string[index].isalpha():
				parts[splitPart] = string[:index]
				parts.insert(splitPart + 1, string[index:])
				break


	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s limit 1000000000")
		sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s AND bible_id='ANYWBT'"))
		filenamesMap = db.selectMapList(sql, (typeCode))
		db.close()

		for prefix in filenamesMap.keys():
			filenames = filenamesMap[prefix]
			(numErrors, template, files) = self.parseFileset(prefix, filenames)
			if numErrors == 0:
				self.parsedList.append((template.name, prefix))
			else:
				self.unparsedList.append((numErrors, template.name, prefix))
				for file in files:
					if len(file.errors) > 0:
						print(template.name, prefix, file.file, ", ".join(file.errors))


	def parseFileset(self, prefix, filenames):
		parserTries = []
		for template in self.audioTemplates:
			(numErrors, template, files) = self.parseOneFileset(template, prefix, filenames)
			if numErrors == 0:
				return (numErrors, template, files)
			parserTries.append((numErrors, template, files))
		best = 1000000
		selected = None
		for aTry in parserTries:
			if aTry[0] < best:
				best = aTry[0]
				selected = aTry
		return selected


	def parseOneFileset(self, template, prefix, filenames):
		numErrors = 0
		files = []
		for filename in filenames:
			file = self.parse(template, filename)
			files.append(file)
			if len(file.errors) > 0:
				numErrors += 1
		return (numErrors, template, files)


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










