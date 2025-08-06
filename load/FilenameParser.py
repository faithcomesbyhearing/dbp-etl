# FilenameParser.py

import io
import sys
import re
from operator import attrgetter
from Booknames import *
from FilenameReducer import *
from SQLUtility import *
from Config import *
from Log import *
from BibleBrainService import BibleBrainService

class Filename:

	def __init__(self, template, filenameTuple):
		self.template = template
		self.file = filenameTuple[0]
		self.length = filenameTuple[1]
		self.datetime = filenameTuple[2]
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
		self.sortSequence = "000.000"


	def setBookSeq(self, bookSeq):
		self.bookSeq = bookSeq
		if not bookSeq.isdigit():
			if bookSeq[0] not in {"A", "B"}:
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

	def setCovenantBookNameById(self, bookId, covenantBookNameMap):
		# retrieve names from DB; otherwise reference from Booknames.Covenant
		bookName = covenantBookNameMap.get(bookId)
		if bookName == None:
			print("WARN: bookname not found in database for id: %s. It will try to fetch name from Booknames().Covenant" % (bookId))
			bookName = Booknames().Covenant(bookId)
			if bookName == None:
				self.errors.append("bookname not found for id: %s" % (bookId))

		self.name = bookName

	# This should only be used in cases where setBookBySeq was used to set bookId
	# Deuterocanon DC 
	def checkBookName(self, name):
		self.name = name
		book = Booknames().usfmBookId(name)
		if book == None:
			self.errors.append("bookname %s is not recognized" % (name))
		elif book != self.bookId and self.bookId != "":
			if self.bookId == "EZA" and book == "EZR":
				return
			if self.bookId == "EZR" and book == "NEH":
				return
			if self.bookId == "ESG" and book == "EST":
				return
			if self.bookId == "DAG" and book == "DAN":
				return
			self.bookId = book
			#self.errors.append("book id by sequence is %s and book id by name is %s" % (self.bookId, book))


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
		self.verseStart = "1"
		self.verseStartNum = 1


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
			if len(verseStart) == 0 or not verseStart[:-1].isdigit() or verseStart[-1] != "b":
				self.errors.append("non-number verse start: %s" % (verseStart))
		self.verseStartNum = int(self.verseStart)

	## This is used to set the verse start number from the verse start string
	def setVerseStartNum(self, verseStartNum):
		self.verseStartNum = verseStartNum

	def setVerseEnd(self, verseEnd):
		self.verseEnd = verseEnd
		if not verseEnd.isdigit() and (not verseEnd or not verseEnd[:-1].isdigit() or verseEnd[-1] != "r"):
			self.errors.append("non-number verse end: %s" % (verseEnd))
		if self.verseStart.isdigit() and self.verseEnd.isdigit():
			startInt = int(self.verseStart)
			endInt = int(self.verseEnd)
			#if startInt >= endInt:
			#	self.errors.append("verse out of sequence start: %s  end: %s" % (self.verseStart, self.verseEnd))
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
		if typ not in {"mp3", "opus", "webm", "html", "mp4", "usx", "json"}:
			self.errors.append("unknown type .%s" % (typ))


	def getSequence(self):
		prefix = self.bookSeq
		if prefix == None or prefix == "":
			prefix = self.fileSeq
		if prefix == None or prefix == "":
			lookup = {"MAT": "B01", "MRK": "B02", "LUK": "B03", "JHN": "B04"}
			prefix = lookup.get(self.bookId, "000")
		#while len(prefix) < 3:
		#	prefix = "0" + prefix
		return prefix


	def setSortSequence(self):
		bookSeq = str(self.getSequence())
		while len(bookSeq) < 3:
			bookSeq = "0" + bookSeq
		chapSeq = self.chapter
		while(len(chapSeq) < 3):
			chapSeq = "0" + chapSeq
		self.sortSequence = bookSeq + "." + chapSeq


	def numErrors(self):
		return len(self.errors)


	def print(self):
		print(self.bookSeq, self.fileSeq, self.bookId, self.chapter, self.name, self.damid, self.type, self.file, self.errors)

	def toString(self):
		print(
		"bookSeq: %s, fileSeq: %s, bookId: %s, chapter: %s, verseStart: %s, verseStartNum: %s, name: %s, damid: %s, type: %s, file: %s, errors: %s" % (
			self.bookSeq, self.fileSeq, self.bookId,
			self.chapter, self.verseStart, self.verseStartNum,
			self.name, self.damid, self.type,
			self.file, self.errors
		))


class FilenameRegex:

	def __init__(self, name, regex):
		self.name = name
		self.regex = re.compile(regex)


	def parse(self, filenameTuple, parser):
		file = Filename(self, filenameTuple)
		match = self.regex.match(filenameTuple[0])
		if match != None:
			if self.name == "text3":
				if match.group(1) != None:
					file.setBookSeq(match.group(1))
				file.setBookId(match.group(2), parser.chapterMap)
				file.setChapter("1", parser.maxChapterMap)
				chapter = str(parser.maxChapterMap.get(match.group(2)))
				file.setChapterEnd(chapter, parser.maxChapterMap)
				file.setType(match.group(3))
			elif self.name == "text4":
				file.setBookSeq(match.group(1))
				file.setBookId(match.group(2), parser.chapterMap)
				file.setChapter(match.group(3), parser.maxChapterMap)
				file.setType(match.group(4))
			elif self.name == "audioStory1":
				file.setFileSeq(match.group(1))
				file.setTitle(match.group(2))
				file.setDamid(match.group(3))
				file.setType(match.group(4))
			elif self.name == "audioStory2":
				file.setFileSeq(match.group(1))
				file.setTitle(match.group(2))
				file.setDamid(match.group(3))
				file.setType(match.group(4))
			elif self.name == "audioStory3":
				file.setFileSeq(match.group(1))
				file.setTitle(match.group(2))
				file.setType(match.group(3))
			else:
				print("ERROR: unknown templated %s" % (self.name))
				sys.exit()
		else:
			file.errors.append("no regex match to %s" % (self.name))
			self.validateCompleteness(file)
		return file


	def validateCompleteness(self, file):
		if "audioStory" not in file.template.name:
			if file.bookId == None or file.bookId == "":
				file.errors.append("book_id is not found")
			if file.chapter == None or file.chapter == "":
					file.errors.append("chapter not found")
			if file.type == "mp4" and file.chapter.isdigit(): ### this is not used??
				if file.verseStart == None or file.verseStart == "":
					file.errors.append("verse start not found")
				if file.verseEnd == None or file.verseEnd == "":
					file.errors.append("verse end not found")



class FilenameParser:

	def __init__(self, config):
		self.config = config
		self.otOrder = None
		self.ntOrder = None
		self.parsedList = []
		self.unparsedList = []
		self.successCount = {}
		self.totalFiles = []

		self.biblebrain_service = BibleBrainService(s3_client=None, base_url=self.config.biblebrain_services_base_url)

		self.textTemplates = (
			## {book order}{book code(USFM)}.usx
			## Example: 001GEN.usx
			FilenameRegex("text3", r"([0-9]{3})?([A-Z0-9]{3}).(usx)"),

			## {seq}{bookid}_{chap}.json
			## Example: 041MRK_012.json
			FilenameRegex("text4", r"([0-9]{3})?([A-Z0-9]{3})_([0-9]{3})?.(json)")
		)

	def process3(self, filesets):
		db = SQLUtility(self.config)
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		self.maxChapterMap = self.chapterMap.copy()
		self.usfx2Map = db.selectMap("SELECT id_usfx, id FROM books", None)
		self.covenantBookNameMap = db.selectMap("SELECT id, notes FROM books where book_group = 'Covenant Film'", None)
		db.close()

		print("\nFound %s filesets to process" % (len(filesets)))
		for inp in filesets:
			prefix = inp.filesetPrefix
			print("FilenameParser: %s typeCode: %s" % (prefix, inp.typeCode))
			logger = Log.getLogger(inp.filesetId)
			# Deprecated: audio and video templates are not used anymore. They are replaced by BibleBrainService
			if inp.typeCode == "audio":
				templates = []
			elif inp.typeCode == "text":
				templates = self.textTemplates
			elif inp.typeCode == "video":
				templates = []
			else:
				print("ERROR: unknown type_code: %s" % (inp.typeCode))
				sys.exit()

			if inp.typeCode == "audio" or inp.typeCode == "video":
				## Validate filesetId
				if inp.filesetId[6:7] not in {"C","N","O","P","S"}:
					logger.message(Log.EROR, "filesetId must be C,N,O,P, or S in 7th position.")
				if inp.filesetId[7:8] not in {"1", "2"}:
					logger.message(Log.EROR, "filesetId must be 1 or 2 in the 8th position.")
				if inp.filesetId[8:10] not in {"DA", "DV"}:
					logger.message(Log.EROR, "filesetId must be DA or DV.")
				if len(inp.filesetId) > 10 and inp.filesetId[10] != "-":
					bitrateSuffix = inp.filesetId[10:12]
					if bitrateSuffix != '' and not bitrateSuffix.isdigit():
						logger.message(Log.EROR, "filesetId positions 11,12 must be a bitrate number if present.")

			self.otOrder = self.OTOrderTemp(inp.filesetId, inp.languageRecord)
			self.ntOrder = self.NTOrderTemp(inp.filesetId, inp.languageRecord)

			if inp.typeCode == "audio" or inp.typeCode == "video":
				(num_errors, files) = self.parse_fileset(inp.filenames())
			else:
				# Deprecated: This function won't be used anymore. It is replaced by parse_fileset which uses BibleBrainService
				(num_errors, files) = self.parseOneFileset3(templates, prefix, inp.filenamesTuple())
				
			self.totalFiles.append((len(files), prefix))
			if num_errors == 0:
				self.parsedList.append((prefix))
			else:
				self.unparsedList.append((num_errors, prefix))

			if inp.typeCode == "audio":
				(extraChapters, missingChapters, missingVerses) = self.checkBookChapter(prefix, files)
			elif inp.typeCode == "video":
				(extraChapters, missingChapters, missingVerses) = self.checkVideoBookChapterVerse(prefix, files)
			elif inp.typeCode == "text":
				(extraChapters, missingChapters, missingVerses) = ([], [], [])

			FilenameReducer.openAcceptErrorSet(self.config)
			reducer = FilenameReducer(self.config, prefix, inp.csvFilename, files, extraChapters, missingChapters, missingVerses)
			# Does not allow continue if there are errors related to the fileset parsing
			# It will write the errors to the error file including the FilenameReducer errors
			if num_errors > 0:
				reducer.writeErrors(logger)
				print("")
				return False

			reducer.process(logger)
		print("")

	# This function uses BibleBrainService to parse the fileset
	def parse_fileset(self, filenames):
		parsed_results = self.biblebrain_service.parse(filenames)

		num_errors = 0
		files = []

		for result in parsed_results:
			file = Filename(None, (result.filename, 0, None))

			if result.pattern is None:
				file.errors.append("No pattern found for filename: %s" % result.filename)
				files.append(file)
				num_errors += 1
				continue

			if result.error is not None and result.error != "":
				file.errors.append(result.error)
				num_errors += 1
				continue

			file.addUnknown(result.pattern.description)

			if result.pattern.media_id is not None and result.pattern.media_id != "":
				file.setDamid(result.pattern.media_id)
			if result.pattern.book_seq is not None and result.pattern.book_seq != "":
				file.setBookSeq(result.pattern.book_seq)
			if result.pattern.book_id is not None and result.pattern.book_id != "":
				file.setBookId(result.pattern.book_id, self.chapterMap)
			if result.pattern.book_name is not None and result.pattern.book_name != "":
				file.checkBookName(result.pattern.book_name)
			
			file.setChapter(result.pattern.chapter, self.maxChapterMap)

			if result.pattern.verse_start is not None and result.pattern.verse_start != "":
				file.setVerseStart(result.pattern.verse_start)
			if result.pattern.verse_end is not None and result.pattern.verse_end != "":
				file.setVerseEnd(result.pattern.verse_end)

			if "covenant" in result.filename.lower():
				# Covenant films always have chapter 1 and verse 1
				video_chapter_and_verse = "1"
				covenant_book_id = result.pattern.book_id
				# Ensure covenant book IDs start with "C"
				if not covenant_book_id.startswith("C"):
					covenant_book_id = "C" + covenant_book_id

				file.setBookId(covenant_book_id, self.chapterMap)
				file.setCovenantBookNameById(file.bookId, self.covenantBookNameMap)
				file.setChapter(video_chapter_and_verse, self.maxChapterMap)
				file.setVerseStart(video_chapter_and_verse)
				file.setVerseEnd(video_chapter_and_verse)

			if result.pattern.chapter_end is not None and result.pattern.chapter_end != "":
				file.setChapterEnd(result.pattern.chapter_end, self.maxChapterMap)
			elif file.chapter.isdigit():
				file.setChapterEnd(file.chapter, self.maxChapterMap)

			file.setType(result.pattern.extension)
			files.append(file)

		return (num_errors, files)

	# Deprecated: This function is not used anymore. It is replaced by parse_fileset which uses BibleBrainService
	# However, it is kept here because biblebrain_service does not cover text files yet.
	def parseOneFileset3(self, templates, prefix, filenamesTuple):
		numErrors = 0
		files = []
		for filenameTuple in filenamesTuple:
			file = self.parseOneFilename3(templates, prefix, filenameTuple)
			files.append(file)
			if file.numErrors() > 0:
				numErrors += 1
				# print(file.template.name, prefix, file.file, ", ".join(file.errors))
			else:
				self.successCount[file.template.name] = self.successCount.get(file.template.name, 0) + 1
		return (numErrors, files)

	# Deprecated: This function is not used anymore. It is replaced by parse_fileset which uses BibleBrainService
	# However, it is kept here because biblebrain_service does not cover text files yet.
	def parseOneFilename3(self, templates, prefix, filenameTuple):
		parserTries = []
		for template in templates:
			file = template.parse(filenameTuple, self)
			# print("error", file.file, template.name, file.errors, file.type)
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
			maxChapter = self.chapterMap[book]
			for index in range(1, maxChapter + 1):
				if index not in chapters:
					missingChapters.append("%s:%d" % (book, index))
			maxChapter = self.maxChapterMap[book]
			for chapter in chapters:
				#print(chapter)
				if chapter > maxChapter:
					extraChapters.append("%s:%d" % (book, chapter))
		return (extraChapters, missingChapters, [])


	def checkVideoBookChapterVerse(self, prefix, files):
		filesSorted = sorted(files, key=attrgetter('bookId', 'chapterNum', 'verseStartNum'))
		books = {}
		for file in filesSorted:
			if file.bookId not in {None, ""}:
				currChaps = books.get(file.bookId, {})
				currVerses = currChaps.get(file.chapterNum, [])
				currVerses.append((file.verseStartNum, file.verseEndNum))
				currChaps[file.chapterNum] = currVerses
				books[file.bookId] = currChaps
		extraChapters = []
		missingChapters = []
		missingVerses = []
		for book, chapters in books.items():
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
		return (extraChapters, missingChapters, missingVerses)


	def NTOrderTemp(self, filesetId, languageRecord):
		# This fileset is missing any NTOrder
		#if filesetId in {"AZEIBTN2DA", "BLGAMBN1DA"}:
		#	return "Russian"
		# Note BLGAMB has different order from 16 than 64
		if filesetId in {"BLGAMBN1DA16"}:
			return "Traditional" # it is Russian in lpts. TODO: Ask Alan
		# if filesetId in {"GAGIBTN2DA", "GAGIBTN2DA16", "GAGIB1N2DA", "GAGIBTN1DA", "GAGIB1N1DA"}:
		# 	return "Russian" # it is Russian in lpts
		if filesetId in {"RUSBIBN1DA", "RUSBIBN1DA16"}:
			return "Traditional" # it is Russian in lpts. TODO: Ask Alan
		# if filesetId in {"RU1IBSN2DA", "RUSSVRN1DA"}:
		# 	return "Russian" # RU1 is not found, RUSSVR is discontinued
		if languageRecord != None and languageRecord.NTOrder() != None:
			return languageRecord.NTOrder()
		else:
			return "Traditional"


	def OTOrderTemp(self, filesetId, languageRecord):
		if filesetId in {"CASNTMP1DA"}:
			return "Hebrew" # there is no OTOrder element. TODO: ask Alan to add
		# These ENGESV filesets are labeled OTOrder = Hebrew
		# if filesetId in {"ENGESVC1DA", "ENGESVC2DA", "ENGESVC2DA16", "ENGESVO1DA", "ENGESVO2DA"}:
		# 	return "Traditional" # it is Masoretic-Christian in lpts, which currently translates to Traditional
		# if filesetId in {"ENGNABC1DA"}:
		# 	return "Catholic" # ENGNAB does not have any OT filesets in lpts
		if filesetId in {"GRKEPTC1DA", "GRKEPTO1DA"}:
			return "Septuagint2"  # there is no OTOrder element. TODO: ask Alan to add
		# if filesetId in {"GRKAVSO1DA"}:
		# 	return "Septuagint" # there is no GRKAVS in lpts or biblebrain
		if filesetId in {"HBRHMTO2DA", "HBRHMTC2DA"}:
			return "Hebrew" # OTOrder is Masoretic-Tanakh. TODO: more analysis needed to determine solution.
		if filesetId in {"TRNNTMP1DA"}:
			return "TRNNTM" # TODO: is TRNNTM actually a book order
		if languageRecord != None and languageRecord.OTOrder() != None:
			return languageRecord.OTOrder()
		else:
			return "Traditional"

	def summary3(self):
		file = io.open("FilenameParser.out", mode="w", encoding="utf-8")
		for entry in self.parsedList:
			file.write("%s\n" % entry)
		file.write("\n\nUnparsed\n\n")
		for entry in self.unparsedList:
			file.write("%d  %s\n" % entry)
			print("Error unparsed: %s -> %s" % entry)
		file.write("\n\nTotal Files\n\n")
		for entry in self.totalFiles:
			file.write("%d  %s\n" % entry)
			print("Total Files: %s -> %s" % entry)
		file.close()
		for parser, count in self.successCount.items():
			print("Success Count: %s -> %s" % (parser, count))

if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from InputProcessor import InputProcessor

	config = Config()
	AWSSession.shared()

	parser = FilenameParser(config)
	bucket = config.s3_bucket
	migration_stage = "B" if os.getenv("DATA_MODEL_MIGRATION_STAGE") == None else os.getenv("DATA_MODEL_MIGRATION_STAGE")
	lpts_xml = config.filename_lpts_xml if migration_stage == "B" else ""

	languageReader = LanguageReaderCreator(migration_stage).create(lpts_xml)
	filenamesMap = InputProcessor.commandLineProcessor(config, AWSSession.shared().s3Client, languageReader)
	parser.process3(filenamesMap)
	parser.summary3()

# time python3 load/FilenameParser.py test s3://etl-development-input/ "Covenant_Manobo, Obo SD_S2OBO_COV"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "Covenant_Aceh SD_S2ACE_COV"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "ENGESVN2DA"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "TGKWBTP2DV"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "Tatar_N2TTRIBT_USX"

# time python3 load/FilenameParser.py test s3://etl-development-input/ "SLUYPMP2DV"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "URIWBTN2DV"
# time python3 load/FilenameParser.py test s3://etl-development-input/ "SPNBDAP2DV"
