# BuildBookNames.py

#class BuildBookNames:

import io
import sys
import re
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
		self.error = None


	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.error)


	def audioParser1(self, filename):
		parts = re.split("[_.]+", filename)
		self.type = parts[-1]
		if any(c.islower() for c in parts[-2]):
			newParts = self.splitDamId(parts[-2])
			parts[-2] = newParts[1]
			parts.insert(-2, newParts[0])
		self.damid = parts[-2]
		if len(parts) > 4:
			if parts[0][0] not in {"A", "B"}:
				self.error = "invalid seq type"
			if not parts[0][1].isdigit():
				self.error = "non-number seq"
			if not parts[1].isdigit():
				self.error = "non-number chap"

			while (len(parts) > 5):
				parts[2] = parts[2] + "_" + parts[3]
				parts.pop(3)
			self.seq = parts[0]
			self.chap = parts[1]
			self.name = parts[2]
		else:
			self.error = "less than 4 parts %s" % filename
		filenameOut = self.seq + self.chap + self.name + self.damid + "." + self.type
		if filenameOut.replace("_", "") != filename.replace("_",""):
			self.error = "Mismatch %s" % (filenameOut)


	#def 
	#	if self.error != None:
	#		print(filename, self.error)


	def splitDamId(self, string):
		for index in range(len(string) -1, 0, -1):
			if string[index].islower():
				return (string[:index + 1], string[index + 1:])


class FilenameParser:

	def __init__(self):
		self.lookup = Lookup()

	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		filenamesMap = db.selectMapList("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name FROM bucket_listing where type_code=%s", (typeCode))
		db.close()
		for prefix in filenamesMap.keys():
			#print(prefix)
			filenames = filenamesMap[prefix]
			for filename in filenames:
				parser = Filename()
				parser.audioParser1(filename)
				if parser.error != None:
					print(prefix, filename, parser.error)


"""
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


"""

parser = FilenameParser()
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







