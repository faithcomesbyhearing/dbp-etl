# audioHLS.py
# given input bibleid[s][,type], create corresponding HLS filesets in DB (no extra files in S3 are needed)

## approach: INSERT/SELECT as we go, risking partial loads and paying sequential overhead, but simplifying bookkeeping
##           support one or many input bibleid's (btw could invoke one bibleid per aws lambda to parallelize large sets)
## feedback: one line per bibleid/fileset, with letters indicating book being worked (or . if book not in fileset), newline at end
# foreach bibleid (list of input bibleid[/fileset])
#   foreach type_fileset (audio drama,nondrama) # DISTINCT(fileset_id) FROM bible_filesets WHERE set_type_code IN ('audio_drama','audio') AND bitrate=64k
#       # wrinkle: use only C fileset if N and O also exists (duplicates!?  verify C's work on live.bible.is and new bible.is app)
#     print("bibleid/type_fileset:", end=" ") # so can tell what program is working on
#     INSERT INTO bible_filesets (eg ENGESVN2SA)
#     stream_hashid = SELECT FROM bible_filesets WHERE id = ENGESVN2SA
#     local_mp3s = get list of local mp3's (download from s3 if needed)
#     foreach chapter # SELECT file_name FROM bible_files WHERE hash_id=hash_id # ensure cannon book/chapter order
#       if new book, print first letter of book (or . for missing books in cannon-order sequence)
#       foreach bitrate_hashid # DISTINCT(description) FROM bible_fileset_tags WHERE hash_id=hashid AND name='bitrate'
#         # do work
#         verify chapter is available in mp3s (eg B01___01_Matthew_____ENGESVN2DA.mp3)
#         use ffprobe to determine bitrate
#         [bytes,offsets,durations] = use ffprobe to determine verse offset, bytes, and duration
#         # save work
#         INSERT INTO bible_files (stream_hashid, eg B01___01_Matthew_____ENGESVN2DA.m3u8)
#         fileid = SELECT FROM bible_files (what we inserted above)
#         INSERT INTO bible_files_stream_bandwidths (fileid, eg B01___01_Matthew_____ENGESVN2DA-bitrate.m3u8, bitrate)
#         bwid = SELECT FROM bible_files_stream_bandwidths (what we inserted above)
#         INSERT INTO bible_files_stream_segments (bwid, timestamps.id, [bytes,offsets,durations])

import re
from Config import *
from SQLUtility import *
import glob
import subprocess
from subprocess import Popen, PIPE
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
		# TODO: get from S3 if needed
		pat="/Users/jrstear/tmp/"+ self.bibleid +"/"+ self.filesetid +"/*.mp3"
		# TODO: sort by ascending cannon book,chap (and use to determine processing order later)
		return sorted(glob.glob(pat)) 

	def get_verse_start_times(self, mp3):
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
				"AND book_id=\'"+ book +"\' AND chapter_start="+ chap + " ORDER BY ft.verse_start"
			times = db.selectList(sql, None)
			# TODO: sanity-check result, eg non-empty list, sequential verse numbers
			# TODO: write to file
		return times

	def hashId(self):
		filesetId = self.filesetid
		bucket = "dbp-prod"
		typeCode = "audio_stream"
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		self.hash_id = md5.hexdigest()[:12]
		return self.hash_id

def get_bitrate(file):
	bitrateRegex = re.compile('.*bit_rate=([0-9]+)')
	cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep bit_rate'
	s = subprocess.run(cmd, shell=True, capture_output=True)
	# TODO: add error checking on above run and below regex
	return bitrateRegex.match(str(s)).group(1)

def get_boundaries(file, times):
	cmd = 'ffprobe -show_frames -select_streams a -of compact -show_entries frame=best_effort_timestamp_time,pkt_pos ' + file
	pipe = Popen(cmd, shell=True, stdout=PIPE)
	i = prevtime = prevpos = 0
	bound = times[i]
	for line in pipe.stdout:
		tm = timesRegex.match(str(line))
		time = float(tm.group(1))
		pos  = int(tm.group(2))
		if (time >= bound):
			duration = time - prevtime
			nbytes = pos - prevpos
			yield [str(duration), str(prevpos), str(nbytes)]
			prevtime, prevpos = time, pos
			if (i+1 != len(times)):
				i += 1
				bound = times[i]
			else: 
				bound = 99999999 # search to end of pipe
	duration = time - prevtime
	nbytes = pos - prevpos
	yield [str(duration), str(prevpos), str(nbytes)]

## initialize
bookChapRegex = re.compile('.*B\d+___(\d+)_([A-Za-z+]*)') # matches: chapter, bookname
basenameRegex = re.compile('(.+)\.mp3')
timesRegex = re.compile('.*best_effort_timestamp_time=([0-9.]+)\|pkt_pos=([0-9]+)')
bitrateRE = re.compile(".*bit_rate=(\d+)")
books = { "Matthew" : "MAT"} # TODO: setup full book dict
config = Config()
db = dbConnection(config).cursor
# TODO: use a /tmp/hls.pid file for sql log to aid debugging?
sqlfile = open("/Users/jrstear/tmp/ENGESV/sql", "w")

## form list of input filesets
# ARGUMENTS can be one or more [dir/]bible_id/fileset_id
# TODO: parse cli arguments rather than hardcode
inputBibles = [ hls('ENGESV','ENGESVN2DA') ]

for bible in inputBibles:
	## identify mp3s and timings (get from S3 and DB if needed?)
	hashid = bible.hashId()
	for mp3 in bible.get_mp3s():
			verse_start_times = bible.get_verse_start_times(mp3)
			verse_start_times_string = ','.join(map(str, verse_start_times))
			basename = basenameRegex.match(mp3).group(1)
			bitrate = get_bitrate(mp3)
			print(mp3 + " bitrate: " + bitrate)
			# TODO: move get_verse_start_times inside get_boundaries
			for dur, off, byt in get_boundaries(mp3, verse_start_times):
				print(" ".join([dur,off,byt]))
				# TODO: note that bible_file_stream_segments has FK to bible_file_timestamps.id,
				# so add timestamps.id to query so we can set that in the INSERT
			print("done")