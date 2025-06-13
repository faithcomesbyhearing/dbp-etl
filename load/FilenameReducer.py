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
import os
from operator import attrgetter
import csv
from datetime import datetime
from Config import *
from Log import *
from FindDuplicateFilesets import *

class FilenameReducer:

	@classmethod
	def openAcceptErrorSet(klass, config):
		klass.config = config
		klass.acceptErrorSet = set()
		fp = open(config.filename_accept_errors, "r")
		for row in fp:
			klass.acceptErrorSet.add(row.strip())
		fp.close()
		for item in klass.acceptErrorSet:
			print("Accept Errors:", item)


	def __init__(self, config, filePrefix, csvFilename, fileList, extraChapters, missingChapters, missingVerses):
		self.config = config
		self.filePrefix = filePrefix
		self.csvFilename = csvFilename
		self.fileList = fileList
		self.extraChapters = extraChapters
		self.missingChapters = missingChapters
		self.missingVerses = missingVerses


	def process(self, logger):
		errorCount = logger.errorCount()

		if self.filePrefix in FilenameReducer.acceptErrorSet:
			acceptedList = self.fileList
			quarantineList = []
		elif errorCount > 0:
			acceptedList = []
			quarantineList = self.fileList
		else:
			acceptedList = self.fileList
			quarantineList = []

		if len(acceptedList) > 0:
			(acceptedList, duplicateList) = self.removeDuplicates(acceptedList)
		else:
			duplicateList = []

		self.writeErrors(logger)
		if len(quarantineList) > 0:
			self.writeOutput("quarantine", quarantineList)
			logger.message(Log.INFO, "%d Files moved to quarantine %d accepted." % (len(quarantineList), len(acceptedList)))
		if len(duplicateList) > 0:
			self.writeOutput("duplicate", duplicateList)
			logger.message(Log.INFO, "%d Files moved to duplicate %d accepted." % (len(duplicateList), len(acceptedList)))
		if len(acceptedList) > 0:
			self.writeOutput("accepted", acceptedList)


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
		"""
		Processes the given list of file objects and writes a CSV output to the designated directory based on listType.
		It calculates the maximum chapter and verse per book to set verse start numbers for "end" chapter files, sorts the file list,
		and then writes the CSV file.

		Parameters:
		- listType: A string indicating the type ("accepted", "duplicate", or "quarantine").
		- fileList: A list of file objects. Each file is expected to have attributes like chapter, bookId, verseEnd, etc.,
					and methods setSortSequence(), setVerseStartNum(), setVerseStart(), and getSequence().
		"""
		if listType == "accepted":
			path = self.config.directory_accepted
		elif listType == "duplicate":
			path = self.config.directory_duplicate
		elif listType == "quarantine":
			path = self.config.directory_quarantine
		else:
			print("ERROR: Unknown listType %s" % (listType))
			sys.exit()

		# Dictionaries to track the maximum chapter and verse per book
		maxVerseByBook = {}
		maxChapterByBook = {}

		# Calculate the maximum chapter and verse for each book.
		# This is used later to set the verse start number for files that are "end" files of a book.
		for f in fileList:
			f.setSortSequence()
			if f.chapter is not None and f.chapter != "end":
				try:
					chapterNum = int(f.chapter)
				except ValueError:
					chapterNum = 0
				maxChapterByBook[f.bookId] = max(maxChapterByBook.get(f.bookId, 0), chapterNum)

				if f.bookId not in maxVerseByBook:
					maxVerseByBook[f.bookId] = {}
				try:
					verseEndInt = int(f.verseEnd) if f.verseEnd else 0
				except ValueError:
					verseEndInt = 0
				# Use the chapter (as a string) as the key, assuming f.chapter is stored as a string
				currentMaxVerse = maxVerseByBook[f.bookId].get(f.chapter, 0)
				maxVerseByBook[f.bookId][f.chapter] = max(currentMaxVerse, verseEndInt)

		## Sort the files by sortSequence
		sortedFileList = sorted(fileList, key=attrgetter('sortSequence'))
		# Set the verse start number for files that are the end of a book
		# We assume that there is only one end file per book
		for f in sortedFileList:
			if f.chapter == "end" and f.bookId in maxChapterByBook and f.bookId in maxVerseByBook:
				# Get the maximum chapter number for the current book
				maxChapterCurrentBook = maxChapterByBook[f.bookId]
				chapterKey = str(maxChapterCurrentBook)  # file.chapter values are strings
				if chapterKey in maxVerseByBook[f.bookId]:
					verseStartNum = maxVerseByBook[f.bookId][chapterKey] + 1
					f.setVerseStartNum(verseStartNum)
					f.setVerseStart(str(verseStartNum))

		filename = os.path.join(path, os.path.basename(self.csvFilename))
		print("FilenameReducer. Create file: ", filename)
		with open(filename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(("type_code", "bible_id", "fileset_id", "sequence", "file_name", "book_id", "book_name",
				"chapter_start", "chapter_end", "verse_start", "verse_sequence", "verse_end", "datetime", "file_size", "errors"))
			## prefix and some fields are redundant
			## optional: bookSeq, fileSeq, name, title, usfx2, damid, filetype
			(typeCode, bibleId, filesetId) = self.filePrefix.split("/")
			for file in sortedFileList:
				writer.writerow((typeCode, bibleId, filesetId, file.getSequence(), 
					file.file, file.bookId, file.name, file.chapter, file.chapterEnd, 
					file.verseStart, file.verseStartNum, file.verseEnd, file.datetime, file.length,
					"; ".join(file.errors)))

	def writeErrors(self, logger):
		logger.fileErrors(self.fileList)

		if self.filePrefix.startswith("video"):
			if len(self.extraChapters) > 0:
				self.summaryMessage(Log.EROR, "chapters too large", self.extraChapters, logger)
			if len(self.missingChapters) > 0:
				self.summaryMessage(Log.WARN, "chapters missing", self.missingChapters, logger)
			if len(self.missingVerses) > 0:
				self.summaryVerseMessage(Log.EROR, "verses missing", self.missingVerses, logger)
		else:
			if len(self.extraChapters) > 0:
				self.summaryMessage(Log.EROR, "chapters too large", self.extraChapters, logger)
			if len(self.missingChapters) > 0:
				self.summaryMessage(Log.WARN, "chapters missing", self.missingChapters, logger)			


	def summaryMessage(self, level, message, errors, logger):
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
		logger.message(level, "%s %s" % (message, ", ".join(results)))


	def appendError(self, results, book, chapStart, chapEnd):
		if chapStart == (chapEnd - 1):
			results.append("%s %d" % (book, chapStart))
		else:
			results.append("%s %d-%d" % (book, chapStart, chapEnd - 1))


	def summaryVerseMessage(self, level, message, errors, logger):
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
		logger.message(level, "%s %s" % (message, ", ".join(results)))


	def appendVerseError(self, results, book, chapStart, verseStart, verseEnd):
		if verseStart == (verseEnd - 1):
			results.append("%s %d:%d" % (book, chapStart, verseStart))
		else:
			results.append("%s %d:%d-%d" % (book, chapStart, verseStart, verseEnd - 1))



