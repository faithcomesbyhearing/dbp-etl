# audioHLS.py
# given input audio filesets, create corresponding HLS, upload to S3, and insert to DB

import re
from Config import *
from SQLUtility import *
import glob
import ffmpeg

class dbConnection:

	def __init__(self, config):
		self.config = config
		self.cursor = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_input_db_name)

class hls:

	def __init__(self, bibleid, filesetid):
		self.bibleid = bibleid
		self.filesetid = filesetid

	def get_mp3s(self):
		# get from S3 if needed
		pat="/Users/jrstear/tmp/"+ self.bibleid +"/"+ self.filesetid +"/*.mp3"
		return sorted(glob.glob(pat)) # optional TODO: sort by book,chap

	def get_verse_start_times(self, mp3): # TODO: not sure this belongs in hls class
		# get from DB if needed
		timingfile = glob.glob(mp3 +".timing")
		# TODO: if timingfile exists, simply read it into an array
		if (len(timingfile) == 0):
			m = mp3Regex.match(mp3)
			# TODO: don't assume match
			chap = str(int(m.group(1))) # omit leading 0's
			book = books[m.group(2)] # TODO: verify dictionary match
			sql = "SELECT ft.timestamp FROM bible_file_timestamps ft " \
				"JOIN bible_files bf ON bf.id=ft.bible_file_id " \
				"JOIN bible_filesets fs ON fs.hash_id=bf.hash_id " \
				"WHERE fs.id=\'"+ self.filesetid +"\' " \
				"AND book_id=\'"+ book +"\' AND chapter_start="+ chap
			start_times = db.selectList(sql, None)
			# TODO: sanity-check result, eg non-empty list, sequential verse numbers
		return start_times

## initialize
mp3Regex = re.compile('.*B\d+___(\d+)_([A-Za-z+]*)') # matches: chapter, bookname
books = { "Matthew" : "MAT"} # TODO: get full book list
config = Config()
db = dbConnection(config).cursor

## form list of input filesets
# ARGUMENTS can be one or more [dir/]bible_id/fileset_id
# should we verify S3 and DB correctness of inputs here? (probably not)
inputBibles = [ hls('ENGESV','ENGESVN2DA') ]

for bible in inputBibles:
	## identify mp3s and timings (get from S3 and DB if needed?)
	for mp3 in bible.get_mp3s():
		try:
			verse_start_times = bible.get_verse_start_times(mp3)
			## make HLS segments
			foo = 'bar'
			## generate SQL

		except Exception as err:
			print("FAILED to process "+ mp3)

	## upload segments to S3
	## insert into DB