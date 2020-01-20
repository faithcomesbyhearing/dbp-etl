# FilenameReducer.py
#
# This program accepts parsed files that have been processed by FilenameParser
# It identifies filesets that have too many errors to process, and drops them 
# into a quarantine list, which becomes a quarantineList CSV file.
# It the identifes duplicates, and choses which item will be used.  
# Files to be used go to processList CSV file and those that are duplicate
# go to the duplicateList CSV.
# 

import sys
import io
import os
from operator import attrgetter
import csv
from datetime import datetime
from Config import *
from FindDuplicateFilesets import *

class FilenameReducer:

	@classmethod
	def openAcceptErrorSet(klass, config):
		klass.config = config
#		errorDir = config.directory_errors
#		pattern = config.filename_datetime 
#		path = errorDir + "Errors-" + datetime.today().strftime(pattern) + ".out"
#		klass.errorFile = open(path, "w")
#		print("openErrorReport", path)
		klass.acceptErrorSet = set()
		fp = open(config.filename_accept_errors, "r")
		for row in fp:
			klass.acceptErrorSet.add(row.strip())
		fp.close()
		for item in klass.acceptErrorSet:
			print("Except Errors:", item)


	def __init__(self, config, filePrefix, fileList, extraChapters, missingChapters, missingVerses):
		self.config = config
		self.filePrefix = filePrefix
		self.fileList = fileList
		self.extraChapters = extraChapters
		self.missingChapters = missingChapters
		self.missingVerses = missingVerses


	def process(self, errorMessages):
		errorCount = len(self.extraChapters) + len(self.missingChapters) + int(len(self.missingVerses) / 20.0)
		for file in self.fileList:
			errorCount += len(file.errors)

		if self.filePrefix in FilenameReducer.acceptErrorSet:
			acceptedList = self.fileList
			quarantineList = []
		else:
			(acceptedList, quarantineList) = self.quarantineErrors(self.fileList, errorCount)

		if len(acceptedList) > 0:
			(acceptedList, duplicateList) = self.removeDuplicates(acceptedList)
		else:
			duplicateList = []

		if errorCount > 0:
			self.writeErrors(errorMessages)
		if len(quarantineList) > 0:
			self.writeOutput("quarantine", quarantineList)
		if len(duplicateList) > 0:
			self.writeOutput("duplicate", duplicateList)
		if len(acceptedList) > 0:
			self.writeOutput("accepted", acceptedList)


	def quarantineErrors(self, fileList, errorCount):
		quarantineList = []
		acceptedList = []
		errPct = 100.00 * errorCount / len(fileList)
		if errPct >= self.config.error_limit_pct:
			quarantineList = fileList
		elif self.filePrefix == "audio/ONBLTC/ONBLTCN2DA16":
			if fileList[0].damid == "ONBLTCN1DA":
				quarantineList = fileList
			else: # damid == ONBLTCN2DA
				acceptedList = fileList
		else:
			acceptedList = fileList
		return (acceptedList, quarantineList)


	def removeDuplicates(self, fileList):
		acceptedList = []
		duplicateList = []		
		uniqueMap = {}
		for file in fileList:
			if file.bookId != None and file.bookId != "":
				key = "%s:%s:%s" % (file.bookId, file.chapter, file.verseStart)
				files = uniqueMap.get(key, [])
				files.append(file)
				uniqueMap[key] = files
			else:
				acceptedList.append(file)

		for key, files in uniqueMap.items():
			if len(files) == 1:
				acceptedList.append(files[0])
			else:
				indexOfLatest = self.findLatest(files)
				for index in range(len(files)):
					if index == indexOfLatest:
						acceptedList.append(files[index])
					else:
						duplicateList.append(files[index])
		return (sorted(acceptedList, key=attrgetter('file')), sorted(duplicateList, key=attrgetter('file')))


	def findLatest(self, files):
		timestamps = []
		for file in files:
			timestamps.append(datetime.fromisoformat(file.datetime))
		latest = max(timestamps)
		earliest = min(timestamps)
		difference = latest - earliest
		if file.type == "mp4":
			limit = 5
		else:
			limit = 1200
		if difference.total_seconds() > limit:
			return timestamps.index(latest)
		else:
			lengths = []
			for file in files:
				lengths.append(len(file.file))
			if file.type == "mp4":
				best = min(lengths)
			else:
				best = max(lengths)
			return lengths.index(best)


	def writeOutput(self, listType, fileList):
		if listType == "accepted":
			path = self.config.directory_accepted
		elif listType == "duplicate":
			path = self.config.directory_duplicate
		elif listType == "quarantine":
			path = self.config.directory_quarantine
		else:
			print("ERROR: Unknown listType %s" % (listType))
			sys.exit()

		filename = path + self.filePrefix.replace("/", "_") + ".csv"
		self.ensureDirectory(filename)

		with open(filename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(("type_code", "bible_id", "fileset_id", "file_name", "book_id", 
				"chapter_start", "chapter_end", "verse_start", "verse_end", "datetime", "file_size", "errors"))
			## prefix and some fields are redundant
			## optional: bookSeq, fileSeq, name, title, usfx2, damid, filetype
			(typeCode, bibleId, filesetId) = self.filePrefix.split("/")
			for file in fileList:
				writer.writerow((typeCode, bibleId, filesetId, file.file, file.bookId, 
					file.chapter, file.chapterEnd, file.verseStart, file.verseEnd, file.datetime, file.length, "; ".join(file.errors)))


	def writeErrors(self, errorMessages):
		for file in self.fileList:
			if len(file.errors) > 0:
				errorMessages.append("%s/%s %s" % (self.filePrefix, file.file, ", ".join(file.errors)))

		if self.filePrefix.startswith("video"):
			if len(self.extraChapters) > 0:
				self.summaryMessage("chapters too large", self.extraChapters, errorMessages)
			if len(self.missingChapters) > 0:
				self.summaryMessage("chapters missing", self.missingChapters, errorMessages)
			if len(self.missingVerses) > 0:
				self.summaryVerseMessage("verses missing", self.missingVerses, errorMessages)
		else:
			if len(self.extraChapters) > 0:
				self.summaryMessage("chapters too large", self.extraChapters, errorMessages)
			if len(self.missingChapters) > 0:
				self.summaryMessage("chapters missing", self.missingChapters, errorMessages)			


	def summaryMessage(self, message, errors, errorMessages):
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
		errorMessages.append("%s %s %s\n" % (self.filePrefix, message, ", ".join(results)))


	def appendError(self, results, book, chapStart, chapEnd):
		if chapStart == (chapEnd - 1):
			results.append("%s %d" % (book, chapStart))
		else:
			results.append("%s %d-%d" % (book, chapStart, chapEnd - 1))


	def summaryVerseMessage(self, message, errors, errorMessages):
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
		errorMessages.append("%s %s %s\n" % (self.filePrefix, message, ", ".join(results)))


	def appendVerseError(self, results, book, chapStart, verseStart, verseEnd):
		if verseStart == (verseEnd - 1):
			results.append("%s %d:%d" % (book, chapStart, verseStart))
		else:
			results.append("%s %d:%d-%d" % (book, chapStart, verseStart, verseEnd - 1))


	## This should be in conf program, not here.
	def ensureDirectory(self, filename):
		dirPath = os.path.dirname(filename)
		#print("dirPath", dirPath)
		if not os.path.exists(dirPath):
			os.makedirs(dirPath)
			#print("mkdirs", dirPath)


