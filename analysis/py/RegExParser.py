# RegExParser

import re
from SQLUtility import *

class RegExParser:


	def __init__(self, typeCode):
		db = SQLUtility("localhost", 3306, "root", "valid_dbp")
		#self.chapterMap = db.selectMap("SELECT id, chapters FROM books", None)
		#self.usfx2Map = db.selectMap("SELECT id_usfx, id FROM books", None)
		#self.usfx2Map['J1'] = '1JN' ## fix incorrect entry in books table
		#sql = ("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
		#	+ " FROM bucket_listing where type_code=%s limit 1000000000")
		#sqlTest = (("SELECT concat(type_code, '/', bible_id, '/', fileset_id), file_name"
		#	+ " FROM bucket_listing where type_code=%s AND bible_id='PORERV'"))
		#filenamesMap = db.selectMapList(sql, (typeCode))
		self.filenameList = db.selectList("SELECT file_name FROM bucket_listing WHERE type_code=%s limit 10000000", (typeCode))
		db.close()


	def parseVideo(self):
		video1 = re.compile(r"(.*)_(MAT|MRK|LUK|JHN)_([0-9]+)-([0-9]+b?)-([0-9]+)(.*)")
		video2 = re.compile(r"(.*)_(MAT|MRK|LUK|JHN)_(End_[Cc]redits)()()(.*)")
		video3 = re.compile(r"(.*)_(Mark)_([0-9]+)-([0-9]+b?)-([0-9]+)(.*)")
		video4 = re.compile(r"(.*)_(MRKZ)_(End_[Cc]redits)()()(.*)")
		regExList = (video1, video2, video3, video4)
		for filename in self.filenameList:
			print(filename)
			match = None
			for regex in regExList:
				match = regex.match(filename)
				if match != None:
					print("video1", match.group(1), match.group(2), match.group(3), match.group(4), 
						match.group(5), match.group(6))
					break
			if match == None:
				print("ERROR: could not parse", filename)
				sys.exit() # terminate if none of the parsers can parse the filename


	def parseText(self):
		## {damid}_{bookseq}_{bookid}_{optionalchap}.html   AAZANT_70_MAT_10.html
		text1 = re.compile(r"([A-Z0-9]+)_([0-9]+)_([0-9A-Z]+)_?([0-9]*).(html)")
		## {usfx2}{optionalchap}.html  AC12.html
		text2 = re.compile(r"()()([A-Z][A-Z0-9])([0-9]*).(html)")
		regExList = (text1, text2)
		for filename in self.filenameList:
			print(filename)
			match = None
			for regex in regExList:
				match = regex.match(filename)
				if match != None:
					print("text1", match.group(1), match.group(2), match.group(3), match.group(4), 
						match.group(5))
					break
			if match == None:
				print("ERROR: could not parse", filename)
				sys.exit() # terminate if none of the parsers can parse the filename


	def parseAudio(self):
		regexList = (
			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_Matthew_____ENGGIDN2DA.mp3
			("audio1", re.compile(r"([AB][0-9]+)_+([0-9]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).mp3")),
			## {bookseq}___{chap}_{bookname}____{damid}.mp3   B01___01_1CorinthiensENGGIDN2DA.mp3
			("audio2", re.compile(r"([AB][0-9]+)_+([0-9]+)_([1-4]?[A-Z][a-z]+)([A-Z0-9]+).mp3")),
			## {bookseq}___{chap}_{bookname1}_{bookname2}____{damid}.mp3   B01___01_San_Mateo___ACCIBSN1DA.mp3			
			("audio3", re.compile(r"([AB][0-9]+)_+([0-9]+)_([1-4]?[A-Za-z]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).mp3")),
			## {bookseq}__{chapter},{endchapter}_{bookname}___{damid}.mp3  A23__009,10_Psalms___ENGNABC1DA.mp3
			("audio4", re.compile(r"([AB][0-9]+)_+([0-9]+),([0-9]+)_([1-4]?[A-Za-z]+)_+([A-Z0-9]+).mp3")),

			## {bookseq}_{bookname}_{chap}_{damid}.mp3   B01_Genesis_01_S1COXWBT.mp3
			("audio5", re.compile(r"([AB][0-9]+)_([1-4]?[A-Za-z]+)_([0-9]+)_([A-Z0-9]+).mp3")),

			## {misc}_{damid}_Set_{fileseq}_{bookname}_{chap}_{verse_start}-{verse_end}.mp3   Nikaraj_P2KFTNIE_Set_051_Luke_21_1-19.mp3
			("audio6", re.compile(r"([A-Za-z]+)_([A-Z0-9]+)_Set_([0-9]+)_([A-Za-z]+)_([0-9]+)_([0-9]+)-([0-9]+).mp3")),

			## {lang}_{vers}_{bookseq}_{bookname}_{chap}_{versestart}-{verseend}_{unknown}_{unknown}.mp3  audio/SNMNVS/SNMNVSP1DA/SNM_NVS_01_Genesis_041_50-57_SET91_PASSAGE1.mp3
			("audio7", re.compile(r"([A-Z]+)_([A-Z]+)_([0-9]+)_([1-4]?[A-Za-z]+)_([0-9]+)_([0-9]+[b]?)-([0-9]+[a]?)_.*.mp3")),

			## {fileseq}_{title}_{damid}.mp3  01_God_S2AIGWBT.mp3
			("audioStory1", re.compile(r"([AB]?[0-9]+)_([A-Za-z0-9_]+)_([A-Z0-9]+).mp3")),
			## {fileseq}_{title}.mp3  01_Creation.mp3
			("audioStory2", re.compile(r"([0-9]+)_([A-Za-z0-9_'\(\)\-& ]+).mp3")),
		)

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
		## Need to somehow lower the priority of this template, so it is only used when others fail totally.
		
		## {fileseq}_{title}.mp3
		##FilenameTemplate("audio_story", ("file_seq", "title", "type"), ()),
		##FilenameTemplate("audio_story2", ("book_seq", "file_seq", "title", "type"), ())		
		for filename in self.filenameList:
			print(filename)
			match = None
			for regex in regexList:
				match = regex[1].match(filename)
				if match != None:
					print(regex[0], match.groups())
					#print(regex[0], match.string)
					#print(regex[0], match.group(1), match.group(2), match.group(3), match.group(4)) 
						#match.group(5))
					break
			if match == None:
				print("ERROR: could not parse", filename)
				if "_" in filename:
					sys.exit() # terminate if none of the parsers can parse the filename



parser = RegExParser('audio')
#parser.parseVideo()
#parser.parseText()
parser.parseAudio()

