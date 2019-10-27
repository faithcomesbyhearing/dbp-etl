# AudioHLS.py

# given input bibleid[s][,type], create corresponding HLS filesets in DB (no extra files in S3 are needed)
#
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
#        # enforce cannon book/chapter order in mp3s or sql query, and foreach on that (for progress monitoring)
#     foreach chapter # SELECT file_name FROM bible_files WHERE hash_id=hash_id
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

import sys
import os
import re
#from Config import *
import boto3
from AudioHLSAdapter import *
import glob
import subprocess
from subprocess import Popen, PIPE
import hashlib

MP3_DIRECTORY = "%s/FCBH/files/tmp" % (os.environ["HOME"])

class AudioHLS:

	def __init__(self):
		self.adapter = AudioHLSAdapter()
		session = boto3.Session(profile_name='FCBH_Gary') # needs config
		self.s3Client = session.client('s3')
		self.bitrateRegex = re.compile(r"bit_rate=([0-9]+)")
		self.timesRegex = re.compile(r"best_effort_timestamp_time=([0-9.]+)\|pkt_pos=([0-9]+)")


	def processLambdaEvent(self, event):
		print("This method will be used to start program from an AWS Lambda event")


	def processCommandLine(self):
		print("do", sys.argv)
		if len(sys.argv) < 2:
			print("ERROR: Enter bibleids or bibleid/filesetids to process on command line.")
			sys.exit()
		arguments = sys.argv[1:]
		self.validateArguments(arguments)
		for arg in arguments:
			parts = arg.split("/")
			print(arg)
			if len(parts) > 1:
				self.processFilesetId(parts[1])
			else:
				self.processBibleId(arg)


	## This is used by both processCommandLine and ProcessLambdaEvent
	def validateArguments(self, arguments):
		errorCount = 0
		for arg in arguments:
			parts = arg.split("/")
			if len(parts) == 1:
				if self.adapter.checkBibleId(arg) < 1:
					errorCount += 1
					print("ERROR: bibleid: %s does not exist." % (arg))
				elif self.adapter.checkBibleTimestamps(arg) < 1:
					errorCount += 1
					print("ERROR: bibleid: %s has no timestamp data." % (arg))
			elif len(parts) == 2:
				if parts[1][8:10] != "DA":
					errorCount += 1
					print("ERROR: filesetid: %s must have DA in 8,9 position.")
				elif self.adapter.checkFilesetId(parts[0], parts[1]) < 1:
					errorCount += 1
					print("ERROR: bibleid/filesetid pair: %s does not exist." % (arg))
				elif self.adapter.checkFilesetTimestamps(parts[1]) < 1:
					errorCount += 1
					print("ERROR: filesetid: %s has no timestamp data." % (arg))
			else:
				errorCount += 1
				print("ERROR: argument: %s is invalid and was not processed." % (arg))
		if errorCount > 0:
			sys.exit()


	def processBibleId(self, bibleId):
		print("do bible", bibleId)
		filesetList = self.adapter.selectFilesetIds(bibleId)
		for filesetId in filesetList:
			self.processFilesetId(bibleId, filesetId)


	def processFilesetId(self, bibleId, origFilesetId):
		print("do fileset", origFilesetId)
		fileset = self.adapter.selectFileset(origFilesetId)
		filesetId = origFilesetId[0:8] + "SA" + origFilesetId[11:]
		assetId = fileset[0]
		setTypeCode = fileset[1]
		setTypeCode = "audio_stream"
		setSizeCode = fileset[2]
		hashId = self.hashId(assetId, filesetId, setTypeCode)

		## Select all needed data before transaction starts
		files = self.adapter.selectBibleFiles(origFilesetId)
		timestampMap = self.adapter.selectTimestamps(origFilesetId)

		self.adapter.beginFilesetInsertTran()
		self.adapter.insertFileset((hashId, filesetId, assetId, setTypeCode, setSizeCode))

		for file in files:
			origFilename = file[0]
			filename = origFilename.split(".")[0] + ".m3u8"
			values = (filename,) + file[1:]
			self.adapter.insertFile(values)

			mp3FilePath = self.getMP3File(assetId, bibleId, origFilesetId, origFilename)
			bitrate = self.getBitrate(mp3FilePath)
			filename = origFilename.split(".")[0] + "-" + str(int(int(bitrate)/1000)) + "kbs.m3u8"
			self.adapter.insertBandwidth((filename, bitrate))

			key = "%s:%s" % (file[1], file[2]) # book:chapter
			timestamps = timestampMap[key]
			#print(key, timestamps)
			for segment in self.getBoundaries(mp3FilePath, timestamps):
				#print(segment)
				self.adapter.addSegment(segment)
			self.adapter.insertSegments()
		self.adapter.commitFilesetInsertTran()


	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def getMP3File(self, s3Bucket, bibleId, filesetId, filename):
		s3Key = "audio/%s/%s/%s" % (bibleId, filesetId, filename)
		#print(s3Key)
		filepath = MP3_DIRECTORY + os.sep + s3Key
		#print(filepath)	
		try:
			if not os.access(filepath, os.R_OK):
				print("does not exist")
				directory = filepath[:filepath.rfind("/")]
				if not os.access(directory, os.R_OK):
					print("dir does not exist")
					os.makedirs(directory)
					print("made directory")
				self.s3Client.download_file(s3Bucket, s3Key, filepath)
				print("download succeeds")
			return filepath
		except Exception as err:
			print("ERROR: Download %s failed with error %s" % (s3Key, err))
			return None	


	def getBitrate(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep bit_rate'
		try:
			response = subprocess.run(cmd, shell=True, capture_output=True)
			result = self.bitrateRegex.search(str(response))
			if result != None:
				return result.group(1)
			else:
				print("ERROR: ffprobe for bitrate failed for %s" % (file))
				return None
		except subprocess.CalledProcessError as err:
			print(err.output)
			return None


	def getBoundaries(self, file, times):
		cmd = 'ffprobe -show_frames -select_streams a -of compact -show_entries frame=best_effort_timestamp_time,pkt_pos ' + file
		try:
			pipe = Popen(cmd, shell=True, stdout=PIPE)
			i = prevtime = prevpos = 0
			(timestamp_id, bound) = times[i]
			for line in pipe.stdout:
				#print("line", line)
				tm = self.timesRegex.search(str(line))
				time = float(tm.group(1))
				pos  = int(tm.group(2))
				if (time >= bound):
					duration = time - prevtime
					nbytes = pos - prevpos
					#yield (timestamp_id, str(duration), str(prevpos), str(nbytes))
					yield (timestamp_id, duration, prevpos, nbytes)
					prevtime, prevpos = time, pos
					if (i+1 != len(times)):
						i += 1
						(timestamp_id, bound) = times[i]
					else: 
						bound = 99999999 # search to end of pipe
			duration = time - prevtime
			nbytes = pos - prevpos
			yield (timestamp_id, duration, prevpos, nbytes)
		except subprocess.CalledProcessError as err:
			print(err.output)
			return None



hls = AudioHLS()
#hls.processCommandLine()
#hls.processBibleId("ENGESV")
hls.processFilesetId("ENGESV", "ENGESVN2DA")

"""
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
"""

"""
SAMPLE LAMBDA PROGRAM
import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')

def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        s3_client.download_file(bucket, key, download_path)
"""
