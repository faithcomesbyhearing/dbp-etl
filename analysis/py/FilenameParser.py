# BuildBookNames.py

#class BuildBookNames:

import io
import sys
from Lookup import *
from SQLUtility import *

class Filename:

	def __init__(self):
		self.book = None
		self.chap = None
		self.seq = None
		self.name = None
		self.damid =  None
		self.type = None
		self.file = None
		self.error = None

	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.error)


class FilenameParser:

	def __init__(self):
		self.lookup = Lookup()

	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		filenamesMap = db.selectMapList("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name FROM bucket_listing where type_code=%s limit 50", (typeCode))
		db.close()
		for prefix in filenamesMap.keys():
			print(prefix)
			filenames = filenamesMap[prefix]
			result = self.audioScanner1(prefix, filenames)
			#results = self.parseAudio1(prefix, filenamesMap[prefix])
			#ans = self.testBookId(prefix, results)
			#if ans != "ok":
			#	self.dumpFilenames(ans, prefix, results)
			#for filename in 
			#print(filename)
			#parts = filename#.split("_")
			#firstChar = parts[0:1]
			#secondChar = parts[1:2]
			#if firstChar in {"A", "B"} and secondChar.isdigit():
			#	print(parts[0:3])
			#	print("  ",parts[6:8])
			#	print("    ", parts[9:23])
			#	#print(parts[3]) sequence
			#	#print(parts[4]) part of name
				#print(parts[6]) if len(parts) > 6 else "No 6"
				#print(parts[9]) if len(parts) > 9 else "no 9" 
				#a = 2

			#else:
			#	print("Incorrect format: ", filename)

	def audioScanner1(self, prefix, filenames):
		IN_TYPE = 1
		IN_DAMID = 2
		IN_NAME = 3
		PRE_CHAP = 4
		IN_CHAP = 5
		PRE_SEQ = 6
		IN_SEQ = 7
		state = IN_TYPE
		file = filenames[2]
		parsed = Filename()
		parsed.file = file
		endPos = len(file) -1
		for index in range(endPos, -1, -1):
			#print(index)
			char = file[index]
			print(index, char, ord(char), state, endPos)
			if state == IN_TYPE:
				if char == ".":
					parsed.type = "%d:%d" % (index + 1, endPos + 1)
					endPos = index
					state = IN_DAMID
			elif state == IN_DAMID:
				if char == "_" or char.islower():
					parsed.damid = "%d:%d" % (index + 1, endPos)
					endPos = index
					state = IN_NAME
			elif state == IN_NAME:
				if char == "_" and index < 10:
					parsed.name = "%d:%d" % (index + 1, endPos + 1)
					state = PRE_CHAP
			elif state == PRE_CHAP:
				if char.isdigit():
					endPos = index
					state = IN_CHAP
			elif state == IN_CHAP:
				if char == "_":
					parsed.chap = "%d:%d" % (index + 1, endPos + 1)
					state = PRE_SEQ
			elif state == PRE_SEQ:
				if char.isdigit():
					endPos = index
					state = IN_SEQ
			#elif state == IN_SEQ:

			#else:
				# error
		parsed.seq = "%d:%d" % (0, endPos)
		parsed.print()
		return parsed	


	def parseAudio1(self, prefix, filenames):
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







