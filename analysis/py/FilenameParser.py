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
		self.seq = ""
		self.name = ""
		self.damid = ""
		self.type = ""
		self.file = ""
		self.error = []


	def setSeq(self, seq):
		self.seq = seq
		if not seq[1:].isdigit():
			self.error.append("non-number seq")


	# Should be used before set chapter
	def setBookName(self, name):
		self.name = name
		bookId = Lookup().usfmBookId(name)
		if bookId == None:
			self.error.append("usfm not found for name")
		else:
			self.setBook(bookId)


	def setBook(self, bookId):
		self.book = bookId
		if bookId not in self.chapterMap.keys():
			self.error.append("usfm code is not valid")


	def setChap(self, chap):
		self.chap = chap
		if not chap.isdigit():
			self.error.append("non-number chap")
		elif self.book != None:
			chapter = self.chapterMap.get(self.book)
			if chapter != None and int(chap) > int(chapter):
				self.error.append("chap too large")



	def setDamid(self, damid):
		self.damid = damid


	def setType(self, typ):
		self.type = typ


	def print(self):
		print(self.seq, self.book, self.chap, self.name, self.damid, self.type, self.file, self.error)


	## {book_seq}___{chap}_{bookname}____{damid}.mp3 e.g. B01___01_Matthew_____ENGGIDN2DA.mp3
	def audioParser1(self, filename):
		parts = re.split("[_.]+", filename)
		self.setType(parts[-1])
		# split name from damid if there is no _ between
		if any(c.islower() for c in parts[-2]):
			newParts = self.splitDamId(parts[-2])
			parts[-2] = newParts[1]
			parts.insert(-2, newParts[0])
		self.setDamid(parts[-2])
		# combine name parts
		if len(parts) > 4:
			while (len(parts) > 5):
				parts[2] = parts[2] + "_" + parts[3]
				parts.pop(3)

			self.setSeq(parts[0])
			self.setBookName(parts[2])
			self.setChap(parts[1])
		else:
			self.error.append("less than 5 parts")
		filenameOut = self.seq + self.chap + self.name + self.damid + "." + self.type
		if filenameOut.replace("_", "") != filename.replace("_",""):
			self.error.append("Mismatch %s" % (filenameOut))


	## {book_seq}_{bookname}_{chap}_{damid}.mp3 e.g. B01_Genesis_01_S1COXWBT.mp3
	def audioParser2(self, filename):
		parts = re.split("[_.]+", filename)
		if len(parts) == 5:
			self.setSeq(parts[0])
			self.setBookName(parts[1])
			self.setChap(parts[2])
			self.setDamid(parts[3])
			self.setType(parts[4])
		else:
			self.error.append("not 5 parts")
		filenameOut = self.seq + self.name + self.chap + self.damid + "." + self.type
		if filenameOut.replace("_", "") != filename.replace("_",""):
			self.error.append("Mismatch %s" % (filenameOut))


	def splitDamId(self, string):
		for index in range(len(string) -1, 0, -1):
			if string[index].islower():
				return (string[:index + 1], string[index + 1:])


class FilenameParser:

	def __init__(self):
		self.parsedList = []
		self.unparsedList = []

	def process(self):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		typeCode = 'audio'
		filenamesMap = db.selectMapList("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name FROM bucket_listing where type_code=%s", (typeCode))
		db.close()
		for prefix in filenamesMap.keys():
			#print(prefix)
			filenames = filenamesMap[prefix]

			numErrors = 0
			for filename in filenames:
				parser = Filename(self.chapterMap)
				parser.audioParser1(filename)
				if len(parser.error) > 0:
					numErrors += 1
					print("audio1", prefix, filename, ", ".join(parser.error))
			if numErrors == 0:
				self.parsedList.append(("audio1", prefix))
			else:
				self.unparsedList.append((numErrors, "audio1", prefix))
				"""
				numError = 0
				for filename in filenames:
					parser = Filename(self.chapterMap)
					parser.audioParser2(filename)
					if len(parser.error) > 0:
						numErrors += 1
						print("audio2", prefix, filename, ", ".join(parser.error))
				if numErrors == 0:
					self.parsedList.append(("audio2", prefix))
				else:
					self.unparsedList.append((numErrors, "audio1", prefix))
				"""


		## Try multiple parse functions
		## for each attempt save a) the total error count, b) function name, c) array of Filename
		## after all functions are completed, pick the one with the best error count
		## among those that capture book and chapter
		## report that one, otherwise
		## if any method returns no errors, stop and save that one
		## if any method returns similar errors, i.e. within 10 or within 10% report both.

## we need a better index on bucket


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









