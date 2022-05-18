# UpdateDBPBibleFileTags

import sys
from Log import *
from Config import *
from SQLUtility import *
from SQLBatchExec import *
from AWSSession import *
import math

class UpdateDBPBibleFileTags:

	def __init__(self, config, db, dbOut):
		self.config = config
		self.db = db 
		self.dbOut = dbOut

	# verseList must be sorted in ascending order
	@staticmethod
	def findVerseClosest(verseList, verse):
		listLen = len(verseList)

		if listLen == 0:
			return None

		if listLen == 1:
			return verseList[0]
		
		pivot = math.floor(listLen/2)
		pivot = int(pivot)

		if verse == verseList[pivot]:
			return verseList[pivot]
		elif verse > verseList[pivot]:
			return UpdateDBPBibleFileTags.findVerseClosest(verseList[pivot+1:len(verseList)], verse)
		elif verse > verseList[pivot-1]:
			return verseList[pivot]

		return UpdateDBPBibleFileTags.findVerseClosest(verseList[0:pivot], verse)		

	def getThumbnailNameFromDict(self, thumbnails, bookId, chapter, verse):
		bookDict = thumbnails.get(bookId) if thumbnails != None else None

		if  bookDict != None:
			chapterDict = bookDict.get(chapter)
			if chapterDict != None:
				thumbnailName = chapterDict.get(verse)

				if thumbnailName != None:
					return thumbnailName
				else:
					verseList = list(chapterDict.keys())
					verseList.sort()
					verseSuggested = UpdateDBPBibleFileTags.findVerseClosest(verseList, verse)
					return chapterDict.get(verseSuggested) if verseSuggested != None else None
			
		return None

	def getBibleFilesByHashId(self, hashId):
		sql = ("SELECT bf.id, bf.book_id, bf.chapter_start, bf.verse_start, bft.tag, bft.value "
				"FROM bible_files bf "
				"LEFT JOIN bible_file_tags bft ON bf.id = bft.file_id AND bft.tag = %s "
				"WHERE bf.hash_id = %s")

		return self.db.select(sql, ("thumbnail", hashId))

	def getThumbnailFilesDictIndexedBookChapterVerse(self, gfThumbnailFiles):
		thumbnails = {}

		for thumbnailFile in gfThumbnailFiles:
			thumbnailFileWithoutExt = thumbnailFile.split(".")[0]
			thumbnailArray = thumbnailFileWithoutExt.split("_")

			if len(thumbnailArray) > 3:
				bookId = thumbnailArray[1].lower()
				chapter = int(thumbnailArray[2])
				verse = int(thumbnailArray[3])

				if thumbnails.get(bookId) == None:
					thumbnails[bookId] = {}
					thumbnails[bookId][chapter] = {}
				elif thumbnails.get(bookId).get(chapter) == None: 
					thumbnails[bookId][chapter] = {}
				
				thumbnails[bookId][chapter][verse] = thumbnailFile
		
		return thumbnails

	def process(self, hashId, inputFileset): 
		inp = inputFileset
		insertRows = []
		updateRows = []

		thumbnails = {}

		thumbnails = self.getThumbnailFilesDictIndexedBookChapterVerse(inp.gfThumbnailFiles())

		### Need to match book_id, chapter_start, verse_start between bible_file data in database and the S3 filename, which needs to be parsed to determine book/chapter/verse
		### filenames from S3 will be of the form gf_luk_12_01.jpg. you'll need to do a case-insensitive compare of the bookname.
		### in some cases, the chapter and verse won't match between the bible_file data and the filename 
		# for example, the GF thumbnails for MAT 12 are for:
		# gf_mat_12_02.jpg (MAT, chapter 12, verse 2)
		# gf_mat_12_22.jpg (MAT, chapter 12, verse 22)
		# gf_mat_12_46.jpg (MAT, chapter 12, verse 46)
		# but the Gospel film segments for fileset AFRLPFP2DV are:
		# MAT 12 1
		# MAT 12 22
		# MAT 12 38
		# MAT 12 12 is easy because it matches - use the thumbnail gf_mat_12_22.jpg
		# for the other two, there is not a direct match on chapter and verse
		# so we need to find the closest verse..
		# for MAT 12 1, we'll need to use gf_mat_12_02.jpg (one verse off, thumbnail verse is greater than bible_file verse)
		# for MAT 12 38, we use gf_mat_12_46.jpg (8 verse difference)

		dbpBibleFiles = self.getBibleFilesByHashId(hashId)

		for (bibleFileId, bookId, chapterStart, verseStart, tag, tagValue) in dbpBibleFiles:
			thumbnailFilename = self.getThumbnailNameFromDict(thumbnails, bookId.lower(), int(chapterStart), int(verseStart))

			if thumbnailFilename != None:
				if tagValue == None:
					insertRows.append((thumbnailFilename, bibleFileId, 'thumbnail', 0))
				else:
					updateRows.append((thumbnailFilename, bibleFileId, tag))

		tableName = "bible_file_tags"
		attrNames = ("value",)
		self.dbOut.insert(tableName, ("file_id", "tag", "admin_only"), attrNames, insertRows)
		self.dbOut.update(tableName, ("file_id", "tag"), attrNames, updateRows)

if (__name__ == '__main__'):
	from LanguageReaderCreator import LanguageReaderCreator
	from PreValidate import *
	from InputFileset import *

	if len(sys.argv) < 4:
		print("Usage: UpdateDBPBibleFileTags.py  your_profile  starting_bible_id   ending_bible_id")
		sys.exit()
	startingBibleId = sys.argv[2]
	endingBibleId = sys.argv[3]

	config = Config.shared()
	db = SQLUtility(config)
	dbOut = SQLBatchExec(config)
	update = UpdateDBPBibleFileTags(config, db, dbOut)
	s3Client = AWSSession.shared().s3Client
	# this is probably only for Stage B - ok to hardcode the stage 
	languageReader = LanguageReaderCreator("B").create(config.filename_lpts_xml)

	# list all objects from thumbnails S3 prefix, convert to python collection
	# s3Thumbnails = s3 listobjects from s3://dbp-vid/video/thumbnails

	sql = ("SELECT c.bible_id, f.id, f.hash_id FROM bible_filesets f, bible_fileset_connections c"
			" WHERE f.hash_id = c.hash_id AND f.set_type_code in ('video_stream')"
			" AND c.bible_id >= %s AND c.bible_id <= %s")

	resultSet = db.select(sql, (startingBibleId, endingBibleId))
	location = "s3://%s" % (config.s3_vid_bucket,)
	preValidate = PreValidate(languageReader, s3Client, location)

	for (bibleId, filesetId, hashId) in resultSet: 
		filesetPath = "video/%s/%s" % (bibleId, filesetId)
		(dataList, messages) = preValidate.validateDBPETL(s3Client, location, filesetId, filesetPath)
		if messages != None and len(messages) > 0:
			Log.addPreValidationErrors(messages)
		if dataList == None or len(dataList) == 0:
			print("NO InputFileset", filesetPath)
		else:
			for data in dataList:
				# print(data.toString()) # usually just one entry for video (I think)
				inp = InputFileset(config, location, data.filesetId, filesetPath, data.damId, 
								data.typeCode, data.bibleId(), data.index, data.languageRecord)
				update.process(hashId, inp) 
				dbOut.displayStatements()
				dbOut.displayCounts()
				dbOut.execute("gf_thumbnail_" + filesetId)
	Log.writeLog(config)

# python3 load/UpdateDBPBibleFileTags.py test starting_bible_id  ending_bible_id
