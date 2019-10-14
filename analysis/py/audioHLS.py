# audioHLS.py
# given input audio filesets, create corresponding HLS, upload to S3, and insert to DB

import re
from Config import *
from SQLUtility import *
import glob
import subprocess
import hashlib

class dbConnection:

	def __init__(self, config):
		self.config = config
		self.cursor = SQLUtility(self.config.database_host, self.config.database_port,
			self.config.database_user, self.config.database_input_db_name)

class hls:

	def __init__(self, bibleid, filesetid):
		self.bibleid = bibleid
		self.filesetid = filesetid
		self.directoryRE = re.compile("(.+)/"+ bibleid +"/"+ filesetid)

	def get_mp3s(self):
		# get from S3 if needed
		pat="/Users/jrstear/tmp/"+ self.bibleid +"/"+ self.filesetid +"/*.mp3"
		return sorted(glob.glob(pat)) # optional TODO: sort by book,chap

	def get_verse_start_times(self, mp3): # TODO: not sure this belongs in hls class
		# get from DB if needed
		timingfile = glob.glob(mp3 +".timing")
		# TODO: if timingfile exists, simply read it into an array
		if (len(timingfile) != 0):
			foo = 1
		else:
			m = bookChapRegex.match(mp3)
			# TODO: don't assume match
			chap = str(int(m.group(1))) # omit leading 0's
			book = books[m.group(2)] # TODO: verify dictionary match
			sql = "SELECT ft.timestamp FROM bible_file_timestamps ft " \
				"JOIN bible_files bf ON bf.id=ft.bible_file_id " \
				"JOIN bible_filesets fs ON fs.hash_id=bf.hash_id " \
				"WHERE fs.id=\'"+ self.filesetid +"\' " \
				"AND book_id=\'"+ book +"\' AND chapter_start="+ chap
			times = db.selectList(sql, None)
			start_times = ','.join(map(str,times))
			# TODO: sanity-check result, eg non-empty list, sequential verse numbers
			# TODO: write to file
		return start_times

	def segments(self, mp3, times, bitrate):
		m = basenameRegex.match(mp3)
		basename = m.group(1)
		s = ffmpeg.input(mp3, "-segment -segment_times "+ times +
			" -acodec aac -segment_list_type m3u8 -b:a "+ bitrate +
			basename +"_"+ bitrate +"_%03d.ts")
		print(s.compile) 

	def hashId(self):
		filesetId = self.filesetid
		bucket = "dbp-prod"
		typeCode = "audio_stream"
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]

## initialize
bookChapRegex = re.compile('.*B\d+___(\d+)_([A-Za-z+]*)') # matches: chapter, bookname
basenameRegex = re.compile('(.+)\.mp3')
bitrateRE = re.compile(".*bit_rate=(\d+)")
books = { "Matthew" : "MAT"} # TODO: get full book list
config = Config()
db = dbConnection(config).cursor
sqlfile = open("/Users/jrstear/tmp/ENGESV/sql", "w")

## form list of input filesets
# ARGUMENTS can be one or more [dir/]bible_id/fileset_id
# should we verify S3 and DB correctness of inputs here? (probably not)
inputBibles = [ hls('ENGESV','ENGESVN2SA') ]

for bible in inputBibles:
	## identify mp3s and timings (get from S3 and DB if needed?)
	for mp3 in bible.get_mp3s():
			verse_start_times = bible.get_verse_start_times(mp3)
			basename = basenameRegex.match(mp3).group(1)
			## make HLS segments
			for bitrate in ["16k", "32k", "64k"]:
				s = subprocess.run("ffmpeg -i "+ mp3 +"-f segment -segment_times "+ verse_start_times +" -acodec aac -segment_list_type m3u8 -b:a "+ bitrate +" B01___01_Matthew_____ENGESVN2DA_"+ bitrate +"_%03d.ts", shell=True, capture_output=True)
				# TODO: verify success

				## generate SQL
				for segment in glob.glob(basename +"_"+ bitrate +"_*.ts"):
					s = subprocess.run("ffprobe -v error -select_streams a -show_entries stream=bit_rate -of default=noprint_wrappers=1 "+segment, shell=True, capture_output=True)
					actual_bitrate = int(bitrateRE.match(str(s.stdout)).group(1))
					hashid = bible.hashId()
					foo = 1


	## upload segments to S3
	## insert into DB