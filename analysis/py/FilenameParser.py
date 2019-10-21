# FilenameParser.py

import io
import sys
import re
from Lookup import *
from SQLUtility import *

class Filename:

	def __init__(self):
		#self.chapterMap = chapterMap
		self.book = ""
		self.chap = ""
		self.verseStart = ""
		self.verseEnd = ""
		self.bookSeq = ""
		self.fileSeq = ""
		self.name = ""
		self.title = ""
		self.usfx2 = ""
		self.damid = ""
		self.unknown = []
		self.type = ""
		self.file = ""
		self.errors = []
		self.template = None


	def setBookSeq(self, bookSeq):
		self.bookSeq = bookSeq
		if not bookSeq.isdigit():
			if not bookSeq[0] in {"A", "B"}:
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
			self.errors.append("usfm not found for name: %s" % (name))
		else:
			self.setBook(bookId)


	def setBook(self, bookId, chapterMap):
		self.book = bookId
		if bookId not in chapterMap.keys():
			self.errors.append("usfm code %s is not valid" % (bookId))


	def setUSFX2(self, usfx2, chapterMap, usfx2Map):
		self.usfx2 = usfx2
		bookId = usfx2Map.get(usfx2)
		self.setBook(bookId, chapterMap)


	def setChap(self, chap, chapterMap):
		self.chap = chap
		if not chap.isdigit():
			self.errors.append("non-number chap")
		elif self.book != None:
			chapter = chapterMap.get(self.book)
			if chapter != None and int(chap) > int(chapter):
				self.errors.append("chap too large: %s for %s" % (chap, self.book))


	def setVerseStart(self, verseStart):
		self.verseStart = verseStart
		if not verseStart.isdigit():
			self.errors.append("non-number verse start: %s" % (verseStart))


	def setVerseEnd(self, verseEnd):
		self.verseEnd = verseEnd
		if not verseEnd.isdigit():
			self.errors.append("non-number verse end: %s" % (verseEnd))


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


	def numErrors(self):
		return len(self.errors)


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
			elif item == "usfx2":
				fileOut.append(self.usfx2)
			elif item == "chapter":
				if not template.optionalChapter or self.chap != "0":
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
		self.chapterPosition = None
		for index in range(len(parts)):
			part = parts[index]
			if part not in {"book_id", "chapter", "verse_start", "verse_end", 
				"book_seq", "file_seq", "book_name", "usfx2", "title", "damid", "type", "misc"}:
				print("ERROR: filenameTemplate part %s is not known" % (part))
				sys.exit()
			if part in {"book_name", "title"}:
				self.namePosition = index
			if part == "chapter":
				self.chapterPosition = index
		self.hasProblemDamId = ("damid_front_clean" in specialInst)
		self.verseEndClean = ("verse_end_clean" in specialInst)
		self.optionalChapter = ("optional_chapter" in specialInst)
		self.splitPosition2 = ("split_position2" in specialInst)


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
			## Need to somehow lower the priority of this template, so it is only used when others fail totally.
			## {fileseq}_{title}.mp3
			##FilenameTemplate("audio10", ("file_seq", "title", "type"), ())
		)
		self.textTemplates = (
			## {damid}_{bookseq}_{bookid}_{optionalchap}.html   AAZANT_70_MAT_10.html
			FilenameTemplate("text1", ("damid", "book_seq", "book_id", "chapter", "type"), ("optional_chapter",)),
			## {usfx2}{optionalchap}.html  AC12.html
			FilenameTemplate("text2", ("usfx2", "chapter", "type"), ("split_position2", "optional_chapter")),
		)
		self.videoTemplates = (
			## {lang}_{book_id}_{chap}-{verse_start}-{verse-end}.mp4  Romanian_MRK_9-33-50.mp4
			FilenameTemplate("video1", ("title", "book_id", "chapter", "verse_start", "verse_end", "type"), ("scan_for_book_id",)),
			## {lang}_{book_id}_End_Credits.mp4  Romanian_MRK_End_Credits.mp4

			## {bookseq}___{chap}_{bookname}___{damid}.mp3  MBCMVAN1DA16/B01___23_S_Mateus____MBCMVAN1DA.mp3
			#FilenameTemplate("video3", ("book_seq", "chapter", "book_name", "damid", "type"), ("damid_front_clean",)),
		)


	def parse(self, template, filename):
		file = Filename()
		file.template = template
		parts = re.split("[_.-]+", filename)
		if template.splitPosition2:
			self.splitPosition(parts, 2)
		if template.hasProblemDamId:
			self.splitDamIdIfNeeded(parts, -2)
		if template.verseEndClean:
			verseEndPos = template.parts.index("verse_end")
			if verseEndPos < len(parts):
				self.splitNumAlpha(parts, verseEndPos)
		if len(parts) > template.numParts and template.namePosition != None:
			self.combineName(parts, template.namePosition, template.numParts)
		if template.optionalChapter and (len(parts) + 1) == template.numParts:
			self.addZeroChapter(parts, template)
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
				elif item == "usfx2":
					file.setUSFX2(part, self.chapterMap, self.usfx2Map)
				elif item == "book_id":
					file.setBook(part, self.chapterMap)
				elif item == "chapter":
					file.setChap(part, self.chapterMap)
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


	def splitPosition(self, parts, position):
		if len(parts[0]) > position:
			first = parts[0]
			parts[0] = first[:position]
			parts.insert(1, first[position:])		


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


	def addZeroChapter(self, parts, template):
		parts.insert(template.chapterPosition, "0")



	def process(self, typeCode):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		self.usfx2Map = db.selectMap("SELECT id_usfx, id FROM books", None)
		self.usfx2Map['J1'] = '1JN' ## fix incorrect entry in books table
		sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s limit 1000000000")
		sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s AND bible_id='PORERV'"))
		filenamesMap = db.selectMapList(sql, (typeCode))
		db.close()

		if typeCode == "audio":
			templates = self.audioTemplates
		elif typeCode == "text":
			templates = self.textTemplates
		elif typeCode == "video":
			templates = self.videoTemplates
		else:
			print("ERROR: unknown type_code: %s" % (typeCode))

		for prefix in filenamesMap.keys():
			filenames = filenamesMap[prefix]
			(numErrors, template, files) = self.parseFileset(templates, prefix, filenames)
			if numErrors == 0:
				self.parsedList.append((template.name, prefix))
			else:
				self.unparsedList.append((numErrors, template.name, prefix))
				for file in files:
					if len(file.errors) > 0:
						print(template.name, prefix, file.file, ", ".join(file.errors))


	def parseFileset(self, templates, prefix, filenames):
		parserTries = []
		for template in templates:
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


	def process2(self, typeCode):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		extras = {"FRT":6, "INT":1, "BAK":2, "LXX":1, "CNC":2, "GLO":26, "TDX":1, "NDX":1, "OTH":5, 
			"XXA":4, "XXB":3, "XXC":1, "XXD":1, "XXE":1, "XXF":1, "XXG":1}
		self.chapterMap.update(extras)
		self.chapterMap["PSA"] = 151
		self.usfx2Map = db.selectMap("SELECT id_usfx, id FROM books", None)
		extras = {"FR":"FRT", "IN":"INT", "BK":"BAK", "CN":"CNC", "GS":"GLO", "TX":"TDX", "OH":"OTH",
			"XA":"XXA", "XB":"XXB", "XC":"XXC", "XD":"XXD", "XE":"XXE", "XF":"XXF", "XG":"XXG"}
		self.usfx2Map.update(extras)
		self.usfx2Map["J1"] = "1JN" ## fix incorrect entry in books table
		sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s limit 1000000000")
		sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
			+ " FROM bucket_listing where type_code=%s AND bible_id='PORERV'"))
		filenamesMap = db.selectMapList(sql, (typeCode))
		db.close()

		if typeCode == "audio":
			templates = self.audioTemplates
		elif typeCode == "text":
			templates = self.textTemplates
		elif typeCode == "video":
			templates = self.videoTemplates
		else:
			print("ERROR: unknown type_code: %s" % (typeCode))

		for prefix in filenamesMap.keys():
			filenames = filenamesMap[prefix]
			(numErrors, files) = self.parseOneFileset2(templates, prefix, filenames)
			if numErrors == 0:
				self.parsedList.append((prefix))
			else:
				self.unparsedList.append((numErrors, prefix))
				#for file in files:
				#	if len(file.errors) > 0:
				#		print(template.name, prefix, file.file, ", ".join(file.errors))


#	def parseFileset2(self, templates, prefix, filenames):
#		parserTries = []
#		for template in templates:
#			(numErrors, template, files) = self.parseOneFileset2(templates, prefix, filenames)
#			if numErrors == 0:
#				return (numErrors, template, files)
#			parserTries.append((numErrors, template, files))
#		best = 1000000
#		selected = None
#		for aTry in parserTries:
#			if aTry[0] < best:
#				best = aTry[0]
#				selected = aTry
#		return selected


	def parseOneFileset2(self, templates, prefix, filenames):
		numErrors = 0
		files = []
		for filename in filenames:
			file = self.parseOneFilename2(templates, prefix, filename)
			files.append(file)
			if file.numErrors() > 0:
				numErrors += 1
				print(file.template.name, prefix, file.file, ", ".join(file.errors))
		return (numErrors, files)


	def parseOneFilename2(self, templates, prefix, filename):
		parserTries = []
		for template in templates:
			file = self.parse(template, filename)
			if file.numErrors == 0:
				return file
			parserTries.append(file)
		best = 1000000
		selected = None
		for file in parserTries:
			if file.numErrors() < best:
				best = file.numErrors()
				selected = file
		return selected


	def summary2(self):
		file = io.open("FilenameParser.out", mode="w", encoding="utf-8")
		for entry in self.parsedList:
			file.write("%s\n" % entry)
		file.write("\n\nUnparsed\n\n")
		for entry in self.unparsedList:
			file.write("%d  %s\n" % entry)
		file.close()


parser = FilenameParser()
parser.process2('text')
parser.summary2()










