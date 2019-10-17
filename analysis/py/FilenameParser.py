# BuildBookNames.py

#class BuildBookNames:

import io
import sys
from Lookup import *
from SQLUtility import *

class Filename:

	def __init__(self):
		self.book = ""
		self.chap = ""
		self.seq = ""
		self.name = ""
		self.damid = ""
		self.type = ""
		self.file = ""
		self.error = ""

	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.error)

class FilenameParser:

	def __init__(self):
		self.bookStart = None
		self.bookEnd = None
		self.chapStart = None
		self.chapEnd = None
		self.seqStart = None
		self.seqEnd = None
		self.nameStart = None
		self.nameEnd = None
		self.damidStart = None
		self.damidEnd = None
		self.typeStart = None
		self.typeEnd = None


	def parse(self, filename):
		parsed = Filename()
		if self.bookStart != None:
			parsed.book = filename[self.bookStart:self.bookEnd]
		if self.chapStart != None:
			parsed.chap = filename[self.chapStart:self.chapEnd].strip("_")
		if self.seqStart != None:
			parsed.seq = filename[self.seqStart:self.seqEnd]
		if self.nameStart != None:
			parsed.name = filename[self.nameStart:self.nameEnd]
		if self.damidStart != None:
			parsed.damid = filename[self.damidStart:self.damidEnd]
		if self.typeStart != None:
			parsed.type = filename[self.typeStart:self.typeEnd]
		self.file = filename
		if self.chapEnd != None and self.seqEnd != None:
			chapPadding = "_" * (self.chapEnd - self.seqEnd - len(parsed.chap))
		else:
			chapPadding = "_"
		namePadding = "_" * (self.nameEnd - self.nameStart - len(parsed.name))
		filenameOut = "%s%s%s_%s%s%s.%s" % (parsed.seq, chapPadding, parsed.chap, parsed.name, namePadding, parsed.damid, parsed.type)
		if filenameOut != filename:
			self.error = "Mismatch %s  %s" % (filename, filenameOut)
			print(self.error)



class FilenameScanner:

	def __init__(self):
		self.lookup = Lookup()

	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		filenamesMap = db.selectMapList("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name FROM bucket_listing where type_code=%s", (typeCode))
		db.close()
		for prefix in filenamesMap.keys():
			print(prefix)
			filenames = filenamesMap[prefix]
			parser = self.audioScanner1(prefix, filenames)
			for filename in filenames:
				parser.parse(filename)


	def audioScanner1(self, prefix, filenames):
		IN_TYPE = 1
		IN_DAMID = 2
		IN_NAME = 3
		PRE_CHAP = 4
		IN_CHAP = 5
		PRE_SEQ = 6
		IN_SEQ = 7
		state = IN_TYPE
		midpoint = int(len(filenames) / 2)
		file = filenames[midpoint]
		parser = FilenameParser()
		parser.file = file
		endPos = len(file) -1
		for index in range(endPos, -1, -1):
			char = file[index]
			if prefix == "audio/ZAITBL/ZAINVSN1DA":
				print(index, char, ord(char), state, endPos)
			if state == IN_TYPE:
				if char == ".":
					parser.typeStart = index + 1
					parser.typeEnd = endPos + 1
					endPos = index
					state = IN_DAMID
			elif state == IN_DAMID:
				if char == "_" or char.islower():
					parser.damidStart = index + 1
					parser.damidEnd = endPos
					endPos = index
					state = IN_NAME
			elif state == IN_NAME:
				if char == "_" and index < 10:
					parser.nameStart = index + 1
					parser.nameEnd = endPos + 1
					state = PRE_CHAP
			elif state == PRE_CHAP:
				if char.isdigit():
					endPos = index
					state = IN_CHAP
			elif state == IN_CHAP:
				if char == "_":
					parser.chapStart = index# + 1
					parser.chapEnd = endPos + 1
					state = PRE_SEQ
			elif state == PRE_SEQ:
				if char.isdigit():
					endPos = index
					state = IN_SEQ
			#elif state == IN_SEQ:

			#else:
				# error
		parser.seqStart = 0 
		parser.seqEnd = endPos + 1
		#parser.print()
		return parser	


	def parseAudio1(self, parseKey, prefix, filenames):
		# {seq}__{chap}_{name}{damid}.mp3
		results = []
		for file in filenames:
			parsed = Filename()
			parsed.seq = file[0:3]
			parsed.chap = file[6:8]
			parsed.name = file[9:21].strip("_")
			parsed.damid = file[23:].split(".")[0]
			parsed.book = self.lookup.usfmBookId(parsed.name)
			parsed.file = file
			padding = "_" * (14 - len(parsed.name))
			formatted = "%s___%s_%s%s%s.mp3" % (parsed.seq, parsed.chap, parsed.name, padding, parsed.damid)
			if file != formatted:
				print("DIFF:", file, formatted)
			else:
				print("OK")
			results.append(parsed)
		return results


	def dumpFilenames(self, error, prefix, filenames):
		print("ERROR:", error, prefix)
		for file in filenames:
			file.print()


	def testBookId(self, prefix, filenames):
		for file in filenames:
			if file.book == None:
				return "bookid_null_err"
			if int(file.chap) > self.chapterMap[file.book]:
				return "chap_too_big_err"

		return "ok"


	#def testChapterId(self, prefix, filenames):
	#	for file in filenames:




parser = FilenameScanner()
parser.process()

 #	def testParse(prefix, parsed):
 #		for file in parsed:
 #			# regorganize these by books
 #			sequence = file[0]
 #			bookId = file[1]
 #			chapter = file[2]
 #			if chapter > chapterMap[bookId]:
 #				return "chapter_seq_err"
 #		return "ok"







