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
import json
import csv
from datetime import datetime

quarantineDir = "/Users/garygriswold/FCBH/files/validate/quarantine/"  ## should these be directories, or should this be part of the name
## so that we can see them side by side.
duplicateDir = "/Users/garygriswold/FCBH/files/validate/duplicate/"
acceptedDir = "/Users/garygriswold/FCBH/files/validate/approved/"
approvedErrorsFile = "/Users/garygriswold/FCBH/files/validate/AcceptErrors.csv"
errorReportDir = "/Users/garygriswold/FCBH/files/validate/"
errorPctLimit = 5.0

class FilenameReducer:

	@classmethod
	def openErrorReport(klass):
		path = errorReportDir + "Errors-" + datetime.today().strftime("%y-%m-%dT%H:%M:%S") + ".out"
		klass.errorFile = open(path, "w")
		klass.errorFile.write("Test line\n")
		print("openErrorReport", path)

	@classmethod
	def closeErrorReport(klass):
		klass.errorFile.close()


	def __init__(self, bucket, filePrefix, fileList, extraChapters, missingChapters, missingVerses):
		self.bucket = bucket
		self.filePrefix = filePrefix
		self.fileList = fileList
		self.extraChapters = extraChapters
		self.missingChapters = missingChapters
		self.missingVerses = missingVerses


	def process(self):
		errorCount = len(self.extraChapters) + len(self.missingChapters) + len(self.missingVerses)
		for file in self.fileList:
			errorCount += len(file.errors)

		if self.overrideQuarantine():
			acceptedList = self.fileList
			quarantineList = []
		else:
			(acceptedList, quarantineList) = self.quarantineErrors(self.fileList, errorCount)

		if len(acceptedList) > 0:
			(acceptedList, duplicateList) = self.removeDuplicates(acceptedList)
		else:
			duplicateList = []

		if errorCount > 0:
			self.writeErrors()
		if len(quarantineList) > 0:
			self.writeOutput("quarantine", quarantineList)
		if len(duplicateList) > 0:
			self.writeOutput("duplicate", duplicateList)
		if len(acceptedList) > 0:
			self.writeOutput("accepted", acceptedList)


	def overrideQuarantine(self):
		## Belongs in config
		if not os.path.isfile(approvedErrorsFile):
			print("%s file does not exist" % (approvedErrorsFile))
			sys.exit()
		with open(approvedErrorsFile, newline='\n') as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				#print("row", row)
				prefix = "/".join(row[1:])
				if row[0] == self.bucket and prefix == self.filePrefix:
					return True
		return False


	def quarantineErrors(self, fileList, errorCount):
		quarantineList = []
		acceptedList = []
		errPct = 100.00 * errorCount / len(fileList)
		if errPct >= errorPctLimit:
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
				# should the key include bucket?
				key = "%s:%s:%s:%s" % (self.filePrefix, file.bookId, file.chapter, file.verseStart)
				files = uniqueMap.get(key, [])
				files.append(file)
				uniqueMap[key] = files
			else:
				acceptedList.append(file)

		for key in uniqueMap.keys():
			files = uniqueMap[key]
			if len(files) == 1:
				acceptedList.append(files[0])
			elif len(files) == 2 and files[0].type == ".html":
				if len(files[0].file) > len(files[1].file):
					acceptedList.append(files[0])
					duplicateList.append(files[1])
				else:
					acceptedList.append(files[1])
					duplicateList.append(files[0])
			elif self.filePrefix == "audio/NTMWBT/NTMWBTN2DA16":
				if len(files[0].file) > len(files[1].file):
					acceptedList.append(files[0])
					duplicateList.append(files[1])
				else:
					acceptedList.append(files[1])
					duplicateList.append(files[0])
			elif self.filePrefix == "audio/CWEPBT/CWEPBTN1DA16":
				if len(files[0].file) < len(files[1].file):
					acceptedList.append(files[0])
					duplicateList.append(files[1])
				else:
					acceptedList.append(files[1])
					duplicateList.append(files[0])
			elif self.filePrefix == "audio/ENGWBT/ENGWBTN2DA":
				if len(files[0].file) < len(files[1].file):
					acceptedList.append(files[0])
					duplicateList.append(files[1])
				else:
					acceptedList.append(files[1])
					duplicateList.append(files[0])					
			else:
				print("Unexpected Duplicate %s" % (key))
				for file in files:
					print(file.file, file.bookId, file.chapter, file.verseStart, file.damid, file.type, file.template.name)
		return (sorted(acceptedList, key=attrgetter('file')), sorted(duplicateList, key=attrgetter('file')))


	def writeOutput(self, listType, fileList):
		if listType == "accepted":
			path = acceptedDir
		elif listType == "duplicate":
			path = duplicateDir
		elif listType == "quarantine":
			path = quarantineDir
		else:
			print("ERROR: Unknown listType %s" % (listType))
			sys.exit()

		filename = path + self.bucket + ":" + self.filePrefix.replace("/", ":") + ".csv"
		## This test needs to be promoted to Config, so that it fails immediately on start
		#if not os.path.isfile(filename):
		#	print("Filename %s does not exist.")
		#	sys.exit()
		self.ensureDirectory(filename)

		with open(filename, 'w', newline='\n') as csvfile:
			writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(["bucket", "typeCode", "bible_id", "fileset_id", "file_name",
				"book_id", "chapter_start", "chapter_end", "verse_start", "verse_end"])
			## prefix and some fields are redundant
			## optional: bookSeq, fileSeq, name, title, usfx2, damid, filetype
			(typeCode, bibleId, filesetId) = self.filePrefix.split("/")
			for file in fileList:
				writer.writerow([self.bucket, typeCode, bibleId, filesetId, file.file,
					file.bookId, file.chapter, file.chapterEnd, file.verseStart, file.verseEnd])


	def writeErrors(self):
		for file in self.fileList:
			if len(file.errors) > 0:
				FilenameReducer.errorFile.write("%s %s %s\n" % (file.template.name, self.filePrefix + " " + file.file, ", ".join(file.errors)))
				#FilenameReducer.errorFile.write("%s %s %s\n" % (self.filePrefix + "/" + file.file, ", ".join(file.errors), file.template.name))

		if self.filePrefix.startswith("video"):
			if len(self.extraChapters) > 0:
				self.summaryMessage("chapters too large", self.extraChapters)
			if len(self.missingChapters) > 0:
				self.summaryMessage("chapters missing", self.missingChapters)
			if len(self.missingVerses) > 0:
				self.summaryVerseMessage("verses missing", self.missingVerses)
		else:
			if len(self.extraChapters) > 0:
				self.summaryMessage("chapters too large", self.extraChapters)
			if len(self.missingChapters) > 0:
				self.summaryMessage("chapters missing", self.missingChapters)			


	def summaryMessage(self, message, errors):
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
		FilenameReducer.errorFile.write("%s %s %s\n" % (self.filePrefix, message, ", ".join(results)))


	def appendError(self, results, book, chapStart, chapEnd):
		if chapStart == (chapEnd - 1):
			results.append("%s %d" % (book, chapStart))
		else:
			results.append("%s %d-%d" % (book, chapStart, chapEnd - 1))


	def summaryVerseMessage(self, message, errors):
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
		FilenameReducer.errorFile.write("%s %s %s\n" % (self.filePrefix, message, ", ".join(results)))


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


