# FilenameParser.py

import io
import sys
import re
from operator import attrgetter
from Booknames import *
from LPTSExtractReader import *
from InputReader import *
from SQLUtility import *

class Filename:

	def __init__(self, template, filename):
		self.template = template
		self.file = filename
		self.bookId = ""
		self.chapter = ""
		self.chapterNum = 0
		self.chapterEnd = ""
		self.chapterEndNum = 0
		self.verseStart = ""
		self.verseStartNum = 0
		self.verseEnd = ""
		self.verseEndNum = 0
		self.bookSeq = ""
		self.fileSeq = ""
		self.name = ""
		self.title = ""
		self.usfx2 = ""
		self.damid = ""
		self.unknown = []
		self.type = ""
		self.errors = []


	def setBookSeq(self, bookSeq):
		self.bookSeq = bookSeq
		if not bookSeq.isdigit():
			if not bookSeq[0] in {"A", "B"}:
				self.errors.append("non-number bookSeq")


	def setBookBySeq(self, bookSeq, otOrder, ntOrder, chapterMap):
		self.bookSeq = bookSeq
		if bookSeq[0] == "A":
			orderFunction = otOrder.replace("-", "") + "OT"	
		elif bookSeq[0] == "B":
			orderFunction = ntOrder.replace("-", "") + "NT"
		else:
			self.errors.append("book sequence must begin A or B")
			return
		booknames = Booknames()
		#print("orderFunction", orderFunction)
		func = getattr(booknames, orderFunction)
		bookId = func(bookSeq)	
		if bookId != None:
			self.setBookId(bookId, chapterMap)


	def setFileSeq(self, fileSeq):
		self.fileSeq = fileSeq
		if not fileSeq.isdigit():
			self.errors.append("non-number fileSeq")


	# Should be used before set chapter
	def setBookName(self, name, chapterMap):
		self.name = name
		bookId = Booknames().usfmBookId(name)
		if bookId == None:
			self.errors.append("usfm not found for name: %s" % (name))
		else:
			self.setBookId(bookId, chapterMap)


	def setBookId(self, bookId, chapterMap):
		self.bookId = bookId
		if bookId not in chapterMap.keys():
			self.errors.append("usfm code %s is not valid" % (bookId))


	def setUSFX2(self, usfx2, chapterMap, usfx2Map):
		self.usfx2 = usfx2
		bookId = usfx2Map.get(usfx2)
		self.setBookId(bookId, chapterMap)


	def setChapter(self, chapter, chapterMap):
		#print("set chapter for ", self.bookId, chapter)
		if chapter == "":
			chapter = "0"
		self.chapter = chapter
		if chapter[:3].lower() == "end":
			self.chapter = "end"
			return
		if not chapter.isdigit():
			self.errors.append("non-number chap")
		elif self.bookId != None:
			maxChapter = chapterMap.get(self.bookId)
			if maxChapter != None and int(chapter) > int(maxChapter):
				self.errors.append("chapter too large: %s for %s" % (chapter, self.bookId))
		self.chapterNum = int(self.chapter)



	def setChapterEnd(self, chapter, chapterMap):
		self.chapterEnd = chapter
		if not chapter.isdigit():
			self.errors.append("non-number chap end")
		elif self.bookId != None:
			maxChapter = chapterMap.get(self.bookId)
			if maxChapter != None and int(chapter) > int(maxChapter):
				self.errors.append("end chapter too large: %s for %s" % (chapter, self.bookId))
		self.chapterEndNum = int(self.chapterEnd)


	def setVerseStart(self, verseStart):
		self.verseStart = verseStart
		if not verseStart.isdigit():
			if not verseStart[:-1].isdigit() or not verseStart[-1] == "b":
				self.errors.append("non-number verse start: %s" % (verseStart))
		self.verseStartNum = int(self.verseStart)


	def setVerseEnd(self, verseEnd):
		self.verseEnd = verseEnd
		if not verseEnd.isdigit():
			if not verseEnd[:-1].isdigit() or not verseEnd[-1] == "r":
				self.errors.append("non-number verse end: %s" % (verseEnd))
		if self.verseStart.isdigit() and self.verseEnd.isdigit():
			startInt = int(self.verseStart)
			endInt = int(self.verseEnd)
			if startInt >= endInt:
				self.errors.append("verse out of sequence start: %s  end: %s" % (self.verseStart, self.verseEnd))
		self.verseEndNum = int(self.verseEnd)



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
		if typ not in {"mp3", "html", "mp4"}:
			self.errors.append("unknown type .%s" % (typ))


	def numErrors(self):
		return len(self.errors)


	def print(self):
		print(self.bookSeq, self.fileSeq, self.bookId, self.chapter, self.name, self.damid, self.type, self.file, self.errors)


class FilenameRegex:

	def __init__(self, name, regex):
		self.name = name
		self.regex = re.compile(regex)


class FilenameParser:

	def __init__(self, config):
		self.config = config
		lptsReader = LPTSExtractReader(config)
		self.lptsFilesetMap = lptsReader.getAllFilesetMap()
		self.otOrder = None
		self.ntOrder = None
		self.parsedList = []
		self.unparsedList = []
		self.successCount = {}

		self.videoTemplates = (
			FilenameRegex("video1", r"(.*)_(MAT|MRK|LUK|JHN)_([0-9]+)-([0-9]+)b?-([0-9]+).*.(mp4)"),
			FilenameRegex("video2", r"(.*)_(MAT|MRK|LUK|JHN)_(End_[Cc]redits)()().*.(mp4)"),
			FilenameRegex("video3", r"(.*)_(Mark)_([0-9]+)-([0-9]+)b?-([0-9]+).*.(mp4)"),
			FilenameRegex("video4", r"(.*)_(MRKZ)_(End_[Cc]redits)()().*.(mp4)"),
		)
		self.textTemplates = (
			## {damid}_{bookseq}_{bookid}_{optionalchap}.html   AAZANT_70_MAT_10.html
			FilenameRegex("text1", r"([A-Z0-9]+)_([0-9]+)_([A-Z0-9]+)_?([0-9]*).(html)"),
			## {usfx2}{optionalchap}.html  AC12.html
			FilenameRegex("text2", r"([A-Z][A-Z0-9])([0-9]*).(html)"),
		)
		self.audioTemplates = (
			## using three did not pick up more than 2, but I think it will.
			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_Matthew_____ENGGIDN2DA.mp3
			FilenameRegex("audio101", r"([AB][0-9]+)_+([0-9]+)_+([1-4]?[A-Za-z\-]+)_+([A-Z0-9]+).(mp3)"),
			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_1CorinthiansENGGIDN2DA.mp3
			FilenameRegex("audio102", r"([AB][0-9]+)_+([0-9]+)_+([1-4]?[A-Za-z\-]+[a-z])([A-Z0-9]+).(mp3)"),
			## {bookseq}___{chap}_{bookname1}_{bookname2}____{damid}.mp3   B01___01_San_Mateo___ACCIBSN1DA.mp3			
			FilenameRegex("audio103", r"([AB][0-9]+)_+([0-9]+)_+([1-4]?[A-Za-z]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).(mp3)"),

## Rename 106, 107, 108 to be a specific name
			## {bookseq}_{bookname}_{chap}_{damid}.mp3   B01_Genesis_01_S1COXWBT.mp3
			FilenameRegex("audio106", r"([AB][0-9]+)_([1-4]?[A-Za-z]+)_([0-9]+)_([A-Z0-9]+).(mp3)"),

			## {misc}_{damid}_Set_{fileseq}_{bookname}_{chap}_{verse_start}-{verse_end}.mp3   Nikaraj_P2KFTNIE_Set_051_Luke_21_1-19.mp3
			FilenameRegex("audio107", r"([A-Za-z]+)_([A-Z0-9]+)_Set_([0-9]+)_([A-Za-z]+)_([0-9]+)_([0-9]+)-([0-9]+).(mp3)"),

			## Set_{fileseq}_{bookname}_{chapter}_{versestart}-{verseend}__{damid}.mp3  Set_003_Luke_01_26-38__YMRWINP1DA.mp3
			FilenameRegex("audio108", r"Set_([0-9]+)_([A-Za-z0-9]+)_([0-9]+)_([0-9]+)-([0-9]+)_+([A-Z0-9]+).(mp3)"),

			## {seq}_{bookid}_{chapter}_{verse}_{verse}_Set|SET_{setnum}_damid.mp3  096_GEN_045_1-15_Set_98____SNMNVSP1DA.mp3
			FilenameRegex("audioSNMNVS", r"([0-9]{3})_([A-Z1-4]{3})_([0-9]{3})_([0-9]+)[abc]?-([0-9]+)[abc]?_S[Ee][Tt]_?[0-9]+_+([A-Z1])+.(mp3)"),

			## B01___{bookseq}_{bookname}_{chapter}_{verse}_{verse}{title}___{damid}.mp3   B01___13_Genesis_9_8_16GodsPromisetoNoah__S1PRSGNN.mp3
			FilenameRegex("audioPRSGNN", r"B01_+([0-9]+)_([A-Za-z]+)_([0-9]+)_.*_+([A-Z0-9]+).(mp3)"),

			## {fileseq}__{chapter}_{bookname}_Capitulo___{damid}.mp3  088__05_2Reyes_Capitulo________NHXNTVS1DA.mp3
			FilenameRegex("audioNHX", r"([0-9]{3})_+([0-9]{2})_([A-Za-z1-4]+)_Capitulo_+([A-Z0-9]+).(mp3)")
		)
#		self.audioTemplates0 = (
#			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_Matthew_____ENGGIDN2DA.mp3
#			FilenameRegex("audio1", r"([AB][0-9]+)_+([0-9]+)_([1-4]?[A-Za-z\-]+[a-z])_*([A-Z0-9]+).(mp3)"),
#			## room for audio 2
#			## {bookseq}___{chap}_{bookname1}_{bookname2}____{damid}.mp3   B01___01_San_Mateo___ACCIBSN1DA.mp3			
#			FilenameRegex("audio3", r"([AB][0-9]+)_+([0-9]+)_([1-4]?[A-Za-z]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).(mp3)"),
#			## {bookseq}_{fileseq}__{bookname}_{chapter}___{damid}.mp3  A27_152__Daniel_01_______S2RAMTBL.mp3
#			FilenameRegex("audio4", r"([AB][0-9]+)_+([0-9]+)_+([A-Za-z0-9]+)_+([0-9]+)_+([A-Z0-9]+).(mp3)"),
#			## {bookseq}__{chapter},{endchapter}_{bookname}___{damid}.mp3  A23__009,10_Psalms___ENGNABC1DA.mp3
#			FilenameRegex("audio5", r"([AB][0-9]+)_+([0-9]+),([0-9]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).(mp3)"),
#
#			## {bookseq}_{bookname}_{chap}_{damid}.mp3   B01_Genesis_01_S1COXWBT.mp3
#			FilenameRegex("audio6", r"([AB][0-9]+)_([1-4]?[A-Za-z]+)_([0-9]+)_([A-Z0-9]+).(mp3)"),
#
#			## {misc}_{damid}_Set_{fileseq}_{bookname}_{chap}_{verse_start}-{verse_end}.mp3   Nikaraj_P2KFTNIE_Set_051_Luke_21_1-19.mp3
#			FilenameRegex("audio7", r"([A-Za-z]+)_([A-Z0-9]+)_Set_([0-9]+)_([A-Za-z]+)_([0-9]+)_([0-9]+)-([0-9]+).(mp3)"),
#
#			## Set_{fileseq}_{bookname}_{chapter}_{versestart}-{verseend}__{damid}.mp3  Set_003_Luke_01_26-38__YMRWINP1DA.mp3
#			FilenameRegex("audio8", r"Set_([0-9]+)_([A-Za-z0-9]+)_([0-9]+)_([0-9]+)-([0-9]+)_+([A-Z0-9]+).(mp3)")
#
#			## {lang}_{vers}_{bookseq}_{bookname}_{chap}_{versestart}-{verseend}_{unknown}_{unknown}.mp3  audio/SNMNVS/SNMNVSP1DA/SNM_NVS_01_Genesis_041_50-57_SET91_PASSAGE1.mp3
#			FilenameRegex("audio9", r"([A-Z]+)_([A-Z]+)_([0-9]+)_([1-4]?[A-Za-z]+)_([0-9]+)_([0-9]+[b]?)-([0-9]+[a]?)_.*.(mp3)"),
#
#			## Set_{fileseq}_{bookname}_{chapter}_{versestart}-{verseend}__{damid}.mp3  Set_117_Luke_23_32-43__YMRWINP1DA.mp3
#			FilenameRegex("audio10", r"Set_([0-9]+)_([A-Za-z]+)_([0-9]+)_([0-9]+)-([0-9]+)_+([A-Z0-9]+).(mp3)"),
#
#			## {fileseq}_{title}_{damid}.mp3  01_God_S2AIGWBT.mp3
#			FilenameRegex("audioStory1", r"([AB]?[0-9]+)_([A-Za-z0-9_]+)_([A-Z0-9]+).(mp3)"),
#			## {fileseq}_{title}.mp3  01_Creation.mp3
#			FilenameRegex("audioStory2", r"([0-9]+)_([A-Za-z0-9_'\(\)\-& ]+).(mp3)"),
#
			## {misc}_{misc}_Set_{fileseq}_{bookname}_{chap}_{verse_start}-{verse_end}.mp3   Nikaraj_P2KFTNIE_Set_051_Luke_21_1-19.mp3
			##FilenameTemplate("audio1", ("misc", "misc", "misc", "file_seq", "book_name", "chapter", "verse_start", "verse_end", "type"), ()),
			## {fileseq}_{USFM}_{chap}_{versestart}-{verseend}_SET_{unknown}___{damid}.mp3   audio/SNMNVS/SNMNVSP1DA16/052_GEN_027_18-29_Set_54____SNMNVSP1DA.mp3
			##FilenameTemplate("audio2", ("file_seq", "book_id", "chapter", "verse_start", "verse_end", "misc", "misc", "damid", "type"), ()),
			## {bookseq}___{fileseq}_{bookname}_{chap}_{startverse}_{endverse}{name}__damid.mp3   audio/PRSGNN/PRSGNNS1DA/B01___22_Genesis_21_1_10BirthofIsaac__S1PRSGNN.mp3
			##FilenameTemplate("audio4", ("book_seq", "file_seq", "book_name", "chapter", "verse_start", "verse_end", "title", "damid", "type"), ("verse_end_clean",)),
			## missing explaination
			##FilenameTemplate("audio5", ("file_seq", "misc", "misc", "misc", "book_name", "chapter", "type"), ()),

			## {bookseq}__{fileseq}_{non-standar-book-id}_{chapter}_{chapter_end}_{damid}.mp3   A01__002_GEN_1_2__S1DESWBT.mp

			## {bookseq}_{fileseq}__{bookname}_{chap}_____{damid}.mp3   A08_073__Ruth_01_________S2RAMTBL.mp3
			##FilenameTemplate("audio8", ("book_seq", "file_seq", "book_name", "chapter", "damid", "type"), ()),

			## {file_seq}_{testament}_{KDZ}_{vers}_{bookname}_{chap}.mp3   1215_O2_KDZ_ESV_PSALM_57.mp3
			##FilenameTemplate("audio9", ("book_seq", "file_seq", "book_name", "chapter", "misc", "damid", "type"), ()),
#		)


	def parse(self, template, filename):
		file = Filename(template, filename)
		match = template.regex.match(filename)
		if match != None:
			if template.name[:3] == "vid":
				file.addUnknown(match.group(1))
				file.setChapter(match.group(3), self.chapterMap)
				file.setType(match.group(6))
				if file.chapter.isdigit():
					file.setVerseStart(match.group(4))
					file.setVerseEnd(match.group(5))
				if template.name in {"video1", "video2"}:
					file.setBookId(match.group(2), self.chapterMap)
				elif template.name == "video3":
					file.setBookName(match.group(2), self.chapterMap)
				elif template.name == "video4":
					bookId = "MRK" if match.group(2) == "MRKZ" else None
					file.setBookId(bookId, self.chapterMap)
	
			elif template.name == "text1":
				file.setDamid(match.group(1))
				file.setBookSeq(match.group(2))
				file.setBookId(match.group(3), self.chapterMap)
				file.setChapter(match.group(4), self.chapterMap)
				file.setType(match.group(5))
			elif template.name == "text2":
				file.setUSFX2(match.group(1), self.chapterMap, self.usfx2Map)
				file.setChapter(match.group(2), self.chapterMap)
				file.setType(match.group(3))

#			elif template.name in {"audio1", "audio2"}:
#				file.setBookSeq(match.group(1))
#				file.setBookName(match.group(3), self.chapterMap)
#				file.setChapter(match.group(2), self.chapterMap)
#				file.setDamid(match.group(4))
#				file.setType(match.group(5))
#			elif template.name == "audio3":
#				file.setBookSeq(match.group(1))
#				file.setBookName(match.group(3) + "_" + match.group(4), self.chapterMap)
#				file.setChapter(match.group(2), self.chapterMap)
#				file.setDamid(match.group(5))
#				file.setType(match.group(6))
#			elif template.name == "audio4":
#				file.setBookSeq(match.group(1))
#				file.setFileSeq(match.group(2))
#				file.setBookName(match.group(3), self.chapterMap)
#				file.setChapter(match.group(4), self.chapterMap)
#				file.setDamid(match.group(5))
#			elif template.name == "audio5":
#				file.setBookSeq(match.group(1))
#				file.setChapterEnd(match.group(3), self.chapterMap)
#				file.setBookName(match.group(4), self.chapterMap)
#				file.setChapter(match.group(2), self.chapterMap)
#				file.setDamid(match.group(5))
#				file.setType(match.group(6))
#			elif template.name == "audio6":
#				file.setBookSeq(match.group(1))
#				file.setBookName(match.group(2), self.chapterMap)
#				file.setChapter(match.group(3), self.chapterMap)
#				file.setDamid(match.group(4))
#				file.setType(match.group(5))
#			elif template.name == "audio7":
#				file.addUnknown(match.group(1))
#				file.setDamid(match.group(2))
#				file.setFileSeq(match.group(3))
#				file.setBookName(match.group(4), self.chapterMap)
#				file.setChapter(match.group(5), self.chapterMap)
#				file.setVerseStart(match.group(6))
#				file.setVerseEnd(match.group(7))
#				file.setType(match.group(8))
#			elif template.name == "audio8":
#				file.setFileSeq(match.group(1))
#				file.setBookName(match.group(2), self.chapterMap)
#				file.setChapter(match.group(3), self.chapterMap)
#				file.setVerseEnd(match.group(4))
#				file.setVerseEnd(match.group(5))
#				file.setDamid(match.group(6))
#				file.setType(match.group(7))
#			elif template.name == "audio9":
#				file.addUnknown(match.group(1))
#				file.addUnknown(match.group(2))
#				file.setBookSeq(match.group(3))
#				file.setBookName(match.group(4), self.chapterMap)
#				file.setChapter(match.group(5), self.chapterMap)
#				file.setVerseStart(match.group(6))
#				file.setVerseEnd(match.group(7))
#				file.setType(match.group(8))
#			elif template.name == "audio10":
#				file.setFileSeq(match.group(1))
#				file.setBookName(match.group(2), self.chapterMap)
#				file.setChapter(match.group(3), self.chapterMap)
#				file.setVerseStart(match.group(4))
#				file.setVerseEnd(match.group(5))
#				file.setDamid(match.group(6))
#			elif template.name == "audioStory1":
#				file.setFileSeq(match.group(1))
#				file.setTitle(match.group(2))
#				file.setDamid(match.group(3))
#				file.setType(match.group(4))
#			elif template.name == "audioStory2":
#				file.setFileSeq(match.group(1))
#				file.setTitle(match.group(2))
#				file.setType(match.group(3))
			elif template.name == "audio101" or template.name == "audio102":
				file.setBookBySeq(match.group(1), self.otOrder, self.ntOrder, self.chapterMap)
				file.setChapter(match.group(2), self.maxChapterMap)
				file.setTitle(match.group(3))
				file.setDamid(match.group(4))
				file.setType(match.group(5))
			elif template.name == "audio103":
				file.setBookBySeq(match.group(1), self.otOrder, self.ntOrder, self.chapterMap)
				file.setChapter(match.group(2), self.maxChapterMap)
				file.setTitle(match.group(3) + "_" + match.group(4))
				file.setDamid(match.group(5))
				file.setType(match.group(6))
			elif template.name == "audio106":
				file.setBookSeq(match.group(1))
				file.setBookName(match.group(2), self.chapterMap)
				file.setChapter(match.group(3), self.maxChapterMap)
				file.setDamid(match.group(4))
				file.setType(match.group(5))
			elif template.name == "audio107":
				file.addUnknown(match.group(1))
				file.setDamid(match.group(2))
				file.setFileSeq(match.group(3))
				file.setBookName(match.group(4), self.chapterMap)
				file.setChapter(match.group(5), self.maxChapterMap)
				file.setVerseStart(match.group(6))
				file.setVerseEnd(match.group(7))
				file.setType(match.group(8))
			elif template.name == "audio108":
				file.setFileSeq(match.group(1))
				file.setBookName(match.group(2), self.chapterMap)
				file.setChapter(match.group(3), self.maxChapterMap)
				file.setVerseEnd(match.group(4))
				file.setVerseEnd(match.group(5))
				file.setDamid(match.group(6))
				file.setType(match.group(7))
			elif template.name == "audioSNMNVS":
				file.setFileSeq(match.group(1))
				file.setBookId(match.group(2), self.chapterMap)
				file.setChapter(match.group(3), self.maxChapterMap)
				file.setVerseStart(match.group(4))
				file.setVerseEnd(match.group(5))
				file.setDamid(match.group(6))
				file.setType(match.group(7))
			elif template.name == "audioPRSGNN":
				file.setFileSeq(match.group(1))
				file.setBookName(match.group(2), self.chapterMap)
				file.setChapter(match.group(3), self.maxChapterMap)
				file.setDamid(match.group(4))
				file.setType(match.group(5))
			elif template.name == "audioNHX":
				file.setFileSeq(match.group(1))
				file.setBookName(match.group(3), self.chapterMap)
				file.setChapter(match.group(2), self.maxChapterMap)
				file.setDamid(match.group(4))
				file.setType(match.group(5))
			else:
				print("ERROR: unknown templated %s" % (template.name))
				sys.exit()
		else:
			file.errors.append("no regex match to %s" % (template.name))
		return file


	def process3(self, typeCode):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		extras = {"FRT":6, "INT":1, "BAK":2, "LXX":1, "CNC":2, "GLO":26, "TDX":1, "NDX":1, "OTH":5, 
			"XXA":4, "XXB":3, "XXC":1, "XXD":1, "XXE":1, "XXF":1, "XXG":1}
		self.chapterMap.update(extras)
		corrections = {"MAL":3, "MAN":1, "PS2":1, "BAR":5, "EZA":9, "SIR":52}
		self.chapterMap.update(corrections)
		self.maxChapterMap = self.chapterMap.copy()
		corrections = {"JOL":4, "PSA":151, "MAL":4, "BAR":6}
		self.maxChapterMap.update(corrections)
		self.usfx2Map = db.selectMap("SELECT id_usfx, id FROM books", None)
		extras = {"FR":"FRT", "IN":"INT", "BK":"BAK", "CN":"CNC", "GS":"GLO", "TX":"TDX", "OH":"OTH",
			"XA":"XXA", "XB":"XXB", "XC":"XXC", "XD":"XXD", "XE":"XXE", "XF":"XXF", "XG":"XXG"}
		self.usfx2Map.update(extras)
		self.usfx2Map["J1"] = "1JN" ## fix incorrect entry in books table
		reader = InputReader(self.config)
		filenamesMap = reader.typeCodeListing(typeCode)
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
			filesetId = prefix.split("/")[2]
			#print("START fileset", filesetId)
			lptsRecord = self.lptsFilesetMap.get(filesetId[0:10], None)
			#self.otOrder = lptsRecord.OTOrderTemp(filesetId) if lptsRecord != None else "Traditional"
			#self.ntOrder = lptsRecord.NTOrderTemp(filesetId) if lptsRecord != None else "Traditional"
			self.otOrder = self.OTOrderTemp(filesetId, lptsRecord)
			self.ntOrder = self.NTOrderTemp(filesetId, lptsRecord)

			filenames = filenamesMap[prefix]
			(numErrors, files) = self.parseOneFileset3(templates, prefix, filenames)
			if numErrors == 0:
				self.parsedList.append((prefix))
			else:
				self.unparsedList.append((numErrors, prefix))

			if typeCode in {"text", "audio"}:
				self.checkBookChapter(prefix, files)
			elif typeCode == "video":
				self.checkVideoBookChapterVerse(prefix, files)


	def parseOneFileset3(self, templates, prefix, filenames):
		numErrors = 0
		files = []
		for filename in filenames:
			file = self.parseOneFilename3(templates, prefix, filename)
			self.validateCompleteness(file)
			files.append(file)
			if file.numErrors() > 0:
				numErrors += 1
				print(file.template.name, prefix, file.file, ", ".join(file.errors))
			else:
				self.successCount[file.template.name] = self.successCount.get(file.template.name, 0) + 1
		return (numErrors, files)


	def parseOneFilename3(self, templates, prefix, filename):
		parserTries = []
		for template in templates:
			file = self.parse(template, filename)
			#print("error", filename, template.name, file.errors, file.type)
			if file.numErrors() == 0:
				return file
			parserTries.append(file)
		best = 1000000
		selected = None
		for file in parserTries:
			if file.numErrors() < best:
				best = file.numErrors()
				selected = file
		return selected


	def validateCompleteness(self, file):
		if file.template.name != "audioStory1" and file.template.name != "audioStory2":
			if file.bookId == None or file.bookId == "":
				file.errors.append("book_id is not found")
			if file.chapter == None or file.chapter == "":
					file.errors.append("chapter not found")
			if file.type == "mp4" and file.chapter.isdigit(): ### this is not used??
				if file.verseStart == None or file.verseStart == "":
					file.errors.append("verse start not found")
				if file.verseEnd == None or file.verseEnd == "":
					file.errors.append("verse end not found")


	def checkBookChapter(self, prefix, files):
		books = {}
		for file in files:
			if file.bookId not in {None, "", "FRT", "INT", "BAK", "LXX", "CNC", "GLO", "TDX", "NDX", "OTH", 
				"XXA", "XXB", "XXC", "XXD", "XXE", "XXF", "XXG"}:
				currChaps = books.get(file.bookId, [])
				currChaps.append(file.chapterNum)
				books[file.bookId] = currChaps
		extraChapters = []
		missingChapters = []
		for book, chapters in books.items():
			#print(book)
			maxChapter = self.chapterMap[book]
			for index in range(1, maxChapter + 1):
				if index not in chapters:
					missingChapters.append("%s:%d" % (book, index))
			maxChapter = self.maxChapterMap[book]
			for chapter in chapters:
				#print(chapter)
				if chapter > maxChapter:
					extraChapters.append("%s:%d" % (book, chapter))
		if len(extraChapters) > 0:
			self.summaryMessage(prefix, "chapters too large", extraChapters)
		if len(missingChapters) > 0:
			self.summaryMessage(prefix, "chapters missing", missingChapters)


	def checkVideoBookChapterVerse(self, prefix, files):
		filesSorted = sorted(files, key=attrgetter('bookId', 'chapterNum', 'verseStartNum'))
		books = {}
		for file in filesSorted:
			currChaps = books.get(file.bookId, {})
			currVerses = currChaps.get(file.chapterNum, [])
			currVerses.append((file.verseStartNum, file.verseEndNum))
			currChaps[file.chapterNum] = currVerses
			books[file.bookId] = currChaps
		extraChapters = []
		missingChapters = []
		missingVerses = []
		for book, chapters in books.items():
			#print(book)
			maxChapter = self.chapterMap[book]
			for index in range(1, maxChapter + 1):
				if index not in chapters:
					missingChapters.append("%s:%d" % (book, index))
			for chapter, verses in chapters.items():
				#print(chapter)
				if chapter > maxChapter:
					extraChapters.append("%s:%d" % (book, chapter))
				nextVerse = 1
				for verseStart, verseEnd in verses:
					#print("verse", verseStart, verseEnd)
					while verseStart > nextVerse:
						missingVerses.append("%s:%d:%d" % (book, chapter, nextVerse))
						nextVerse += 1
					nextVerse = verseEnd + 1
		if len(extraChapters) > 0:
			self.summaryMessage(prefix, "chapters too large", extraChapters)
		if len(missingChapters) > 0:
			self.summaryMessage(prefix, "chapters missing", missingChapters)
		if len(missingVerses) > 0:
			self.summaryVerseMessage(prefix, "verses missing", missingVerses)


	def summaryMessage(self, prefix, message, errors):
		#print(prefix, message, errors)
		currBook, chapter = errors[0].split(":")
		startChap = int(chapter)
		nextChap = startChap
		results = []
		for error in errors:
			book, chapter = error.split(":")
			if book == currBook and int(chapter) == nextChap:
				nextChap += 1
			else:
				self.appendError(results, currBook, startChap, nextChap)
				currBook = book
				startChap = int(chapter)
				nextChap = startChap + 1
		self.appendError(results, currBook, startChap, nextChap)
		print(prefix, message, ", ".join(results))


	def appendError(self, results, book, chapStart, chapEnd):
		if chapStart == (chapEnd - 1):
			results.append("%s %d" % (book, chapStart))
		else:
			results.append("%s %d-%d" % (book, chapStart, chapEnd - 1))


	def summaryVerseMessage(self, prefix, message, errors):
		#print(prefix, message, errors)
		currBook, chapter, verse = errors[0].split(":")
		startChap = int(chapter)
		startVerse = int(verse)
		nextVerse = startVerse
		results = []
		for error in errors:
			book, chapter, verse = error.split(":")
			if book == currBook and int(chapter) == startChap and int(verse) == nextVerse:
				nextVerse += 1
			else:
				self.appendVerseError(results, currBook, startChap, startVerse, nextVerse)
				currBook = book
				startChap = int(chapter)
				startVerse = int(verse)
				nextVerse = startVerse + 1
		self.appendVerseError(results, currBook, startChap, startVerse, nextVerse)
		print(prefix, message, ", ".join(results))


	def appendVerseError(self, results, book, chapStart, verseStart, verseEnd):
		if verseStart == (verseEnd - 1):
			results.append("%s %d:%d" % (book, chapStart, verseStart))
		else:
			results.append("%s %d:%d-%d" % (book, chapStart, verseStart, verseEnd - 1))


	def summary3(self):
		file = io.open("FilenameParser.out", mode="w", encoding="utf-8")
		for entry in self.parsedList:
			file.write("%s\n" % entry)
		file.write("\n\nUnparsed\n\n")
		for entry in self.unparsedList:
			file.write("%d  %s\n" % entry)
		file.close()
		for parser, count in self.successCount.items():
			print("Success Count: %s -> %s" % (parser, count))


	def NTOrderTemp(self, filesetId, lptsRecord):
		# This fileset is missing any NTOrder
		if filesetId in {"AZEIBTN2DA", "BLGAMBN1DA"}:
			return "Russian"
		# Note BLGAMB has different order from 16 than 64
		if filesetId in {"BLGAMBN1DA16"}:
			return "Traditional"
		#return self.NTOrder()
		if filesetId in {"RUSBIBN1DA", "RUSBIBN1DA16"}:
			return "Traditional"
		if filesetId in {"RU1IBSN2DA", "RUSSVRN1DA"}:
			return "Russian"
		if lptsRecord != None:
			return lptsRecord.NTOrder()
		else:
			return "Traditional"


	def OTOrderTemp(self, filesetId, lptsRecord):
		if filesetId in {"CASNTMP1DA"}:
			return "Hebrew"
		# These ENGESV filesets are labeled OTOrder = Hebrew
		if filesetId in {"ENGESVC1DA", "ENGESVC2DA", "ENGESVC2DA16", "ENGESVO1DA", "ENGESVO2DA"}:
			return "Traditional"
		if filesetId in {"ENGNABC1DA"}:
			return "Catholic"
		if filesetId in {"GRKEPTC1DA", "GRKEPTO1DA"}:
			return "Septuagint"
		if filesetId in {"HBRHMTO2DA", "HBRHMTC2DA"}:
			return "Hebrew"
		if filesetId in {"TRNNTMP1DA"}:
			return "TRNNTM"
		if lptsRecord != None:
			return lptsRecord.OTOrder()
		else:
			return "Traditional"

config = Config()
parser = FilenameParser(config)
parser.process3('audio')
parser.summary3()










