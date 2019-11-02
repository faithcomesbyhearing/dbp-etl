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
import pymysql
import boto3
import subprocess
from subprocess import Popen, PIPE
import hashlib

## insert into bible_fileset_types (set_type_code, name) values ('audio_stream','HLS Audio Stream');
## insert into bible_fileset_types (set_type_code, name) values ('audio_drama_stre', 'HLS Audio Stream Drama');

HLS_HOST = "localhost"
HLS_USER = "root"
HLS_PORT = 3306
HLS_DB_NAME = "hls_dbp"
HLS_CODEC = "avc1.4d001f,mp4a.40.2"
HLS_STREAM = "1"
HLS_MP3_DIRECTORY = "%s/FCBH/files/tmp" % (os.environ["HOME"])
HLS_AWS_PROFILE = "FCBH_Gary"


## Lambda entry point
def handler(event, context):
	print("This method will be used to start program from an AWS Lambda event")
    #for record in event['Records']:
    #    bucket = record['s3']['bucket']['name']
    #    key = unquote_plus(record['s3']['object']['key'])
    #    download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    #    s3_client.download_file(bucket, key, download_path)
    #    # pass to validateArguments
    #    # call processFilesetId and processBibleId as needed

class AudioHLS:

	def __init__(self):
		self.adapter = AudioHLSAdapter() # This class is found later in this file
		session = boto3.Session(profile_name=HLS_AWS_PROFILE) # needs config
		self.s3Client = session.client("s3")
		self.bitrateRegex = re.compile(r"bit_rate=([0-9]+)")
		self.timesRegex = re.compile(r"best_effort_timestamp_time=([0-9.]+)\|pkt_pos=([0-9]+)")


	def close(self):
		self.adapter.close()


	## Command line entry point
	def processCommandLine(self):
		if len(sys.argv) < 2:
			print("ERROR: Enter bibleids or bibleid/filesetids to process on command line.")
			sys.exit()
		arguments = sys.argv[1:]
		self.validateArguments(arguments)
		for arg in arguments:
			parts = arg.split("/")
			if len(parts) > 1:
				self.processFilesetId(parts[0], parts[1])
			else:
				self.processBibleId(arg)
		self.close()


	## This is used by both processCommandLine and lambda handler
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
		filesetList = self.adapter.selectFilesetIds(bibleId)
		for filesetId in filesetList:
			self.processFilesetId(bibleId, filesetId)


	def processFilesetId(self, bibleId, origFilesetId):
		origFilename = None
		try:
			print("%s/%s: " % (bibleId, origFilesetId), end="", flush=True)
			fileset = self.adapter.selectFileset(origFilesetId)
			filesetId = origFilesetId[0:8] + "SA"
			assetId = fileset[0]
			if fileset[1] == "audio":
				setTypeCode = "audio_stream"
			elif fileset[1] == "audio_drama":
				setTypeCode = "audio_drama_stre"
			else:
				raise("Invalid set_type_code %s" % (fileset[1]))
			setSizeCode = fileset[2]
			hashId = self.hashId(assetId, filesetId, setTypeCode)
			bitrate = origFilesetId[10:] if origFilesetId[10:] != "" else "64"

			## Select all needed data before transaction starts
			files = self.adapter.selectBibleFiles(origFilesetId)
			timestampMap = self.adapter.selectTimestamps(origFilesetId)

			self.adapter.beginFilesetInsertTran()
			self.adapter.replaceFileset((hashId, filesetId, assetId, setTypeCode, setSizeCode))
			self.adapter.deleteFilesBandwidthsSegments(filesetId, bitrate)

			currBook = None
			for file in files:
				if currBook != file[1]:
					currBook = file[1]
					print(currBook[0], end="", flush=True)
				origFilename = file[0]
				filename = origFilename.split(".")[0][:-2] + "SA.m3u8"
				values = (filename,) + file[1:]
				self.adapter.replaceFile(values)

				mp3FilePath = self.getMP3File(assetId, bibleId, origFilesetId, origFilename)
				bitrate = self.getBitrate(mp3FilePath)
				filename = filename.split(".")[0] + "-" + str(int(int(bitrate)/1000)) + "kbs.m3u8"
				self.adapter.insertBandwidth((filename, bitrate))

				key = "%s:%s" % (file[1], file[2]) # book:chapter
				timestamps = timestampMap[key]
				for segment in self.getBoundaries(mp3FilePath, timestamps):
					self.adapter.addSegment(segment)
				self.adapter.insertSegments()
			self.adapter.commitFilesetInsertTran()
			print("", end="\n", flush=True)
		except Exception as error:
			print(("\nERROR: at %s %s" % (origFilename, error)), flush=True)
			self.adapter.rollbackFilesetInsertTran()
			#raise error ## Comment out for production


	def hashId(self, bucket, filesetId, typeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(typeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


	def getMP3File(self, s3Bucket, bibleId, filesetId, filename):
		s3Key = "audio/%s/%s/%s" % (bibleId, filesetId, filename)
		filepath = HLS_MP3_DIRECTORY + os.sep + s3Key
		if not os.access(filepath, os.R_OK):
			#print("Must download %s" % (s3Key))
			directory = filepath[:filepath.rfind("/")]
			if not os.access(directory, os.R_OK):
				os.makedirs(directory)
			self.s3Client.download_file(s3Bucket, s3Key, filepath)
		return filepath


	def getBitrate(self, file):
		cmd = 'ffprobe -select_streams a -v error -show_format ' + file + ' | grep bit_rate'
		response = subprocess.run(cmd, shell=True, capture_output=True)
		result = self.bitrateRegex.search(str(response))
		if result != None:
			return result.group(1)
		else:
			raise Exception("ffprobe for bitrate failed for %s" % (file))


	def getBoundaries(self, file, times):
		cmd = 'ffprobe -show_frames -select_streams a -of compact -show_entries frame=best_effort_timestamp_time,pkt_pos ' + file
		pipe = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
		i = prevtime = prevpos = time = pos = 0
		(timestamp_id, bound) = times[i]
		hasResults = False
		for line in pipe.stdout:
			hasResults = True
			tm = self.timesRegex.search(str(line))
			time = float(tm.group(1))
			pos  = int(tm.group(2))
			if (time >= bound):
				duration = time - prevtime
				nbytes = pos - prevpos
				yield (timestamp_id, duration, prevpos, nbytes)
				prevtime, prevpos = time, pos
				if (i+1 != len(times)):
					i += 1
					(timestamp_id, bound) = times[i]
				else: 
					bound = 99999999 # search to end of pipe
		if not hasResults:
			raise Exception("ffprobe failed to return position data.")
		duration = time - prevtime
		nbytes = pos - prevpos
		yield (timestamp_id, duration, prevpos, nbytes)



class AudioHLSAdapter:

	def __init__(self):
		self.db = pymysql.connect(host = HLS_HOST,
                             		user = HLS_USER,
                             		password = os.environ['MYSQL_PASSWD'],
                             		db = HLS_DB_NAME,
                             		port = HLS_PORT,
                             		charset = 'utf8mb4',
                             		cursorclass = pymysql.cursors.Cursor)
		self.tranCursor = None
		self.currHashId = None
		self.currFileId = None
		self.currBandwidthId = None
		self.segments = []
		self.sqlLog = open("AudioHLSSQL.log", mode="wt")


	def close(self):
		if self.db != None:
			self.db.close()
			self.db = None
		if self.sqlLog != None:
			self.sqlLog.close()
			self.sqlLog = None


	## Query finds audio filesets without HLS where timestamp data is available
	## Limitation: It cannot distinquish between a fileset that has a complete HLS and one with a little
	## This is not being used
	def findFilesetIdNeedHLS(self):
		sql = ("SELECT DISTINCT bs.id"
			" FROM bible_file_timestamps bft, bible_files bf, bible_filesets bs"
			" WHERE bft.bible_file_id = bf.id AND bf.hash_id = bs.hash_id"
			" AND bs.id NOT IN (SELECT CONCAT(LEFT(id,8),'DA',MID(id,11,2))"
			" FROM bible_filesets WHERE set_type_code = 'audio_stream')")
		return self.selectList(sql, None)


	def checkBibleId(self, bibleId):
		sql = "SELECT count(*) FROM bible_fileset_connections WHERE bible_id=%s"
		return self.selectScalar(sql, (bibleId,))


	def checkFilesetId(self, bibleId, filesetId):
		sql = ("SELECT count(*) FROM bible_fileset_connections bfc, bible_filesets bf"
			" WHERE bfc.bible_id=%s AND bf.id=%s")
		return self.selectScalar(sql, (bibleId, filesetId))


	def checkBibleTimestamps(self, bibleId):
		sql = ("SELECT count(*) FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs,"
			" bible_fileset_connections bfc"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id AND bfc.hash_id=bfs.hash_id"
			" AND bfc.bible_id=%s")
		return self.selectScalar(sql, (bibleId,))


	## Returns the number of rows of timestamp data available for a filesetId
	def checkFilesetTimestamps(self, filesetId):
		sql = ("SELECT count(*) FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id AND bfs.id=%s")
		return self.selectScalar(sql, (filesetId,))


	## Returns the audio fileset_ids for a bible_id
	def selectFilesetIds(self, bibleId):
		sql = ("SELECT bf.id FROM bible_filesets bf, bible_fileset_connections bfc"
			" WHERE bf.hash_id=bfc.hash_id AND bfc.bible_id=%s"
			" AND bf.set_type_code IN ('audio', 'audio_drama')")
		return self.selectList(sql, (bibleId,))


	def selectFileset(self, filesetId):
		sql = "SELECT asset_id, set_type_code, set_size_code FROM bible_filesets WHERE id=%s"
		return self.selectRow(sql, (filesetId,))


    ## Returns the files associated with a fileset
	def selectBibleFiles(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, bf.chapter_end, bf.verse_start," 
			" bf.verse_end, bf.file_size, bf.duration"
			" FROM bible_files bf, bible_filesets bfs"
			" WHERE bf.hash_id=bfs.hash_id AND bfs.id=%s"
			" ORDER BY bf.file_name")
		return self.select(sql, (filesetId,))


    ## Returns the timestamps of a fileset in a map[book:chapter:verse] = timestamp
	def selectTimestamps(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, ft.id, ft.verse_start, ft.timestamp"
			" FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id"
			" AND bfs.id=%s ORDER BY bf.file_name, ft.verse_start")
		resultSet = self.select(sql, (filesetId,))
		results = {}
		for row in resultSet:
			key = "%s:%d" % (row[1], row[2]) # book:chapter
			times = results.get(key, [])
			times.append((row[3], row[5]))
			results[key] = times
		return results


	def beginFilesetInsertTran(self):
		try:
			self.db.begin()
			self.tranCursor = self.db.cursor()
		except Exception as err:
			self.error(None, sql, err)


    ## Inserts a new row into the fileset table
	def replaceFileset(self, values):
		sql = ("REPLACE INTO bible_filesets (hash_id, id, asset_id, set_type_code, set_size_code)"
			" VALUES (%s, %s, %s, %s, %s)")
		try:
			self.sqlLog.write((sql + "\n") % values)
			self.tranCursor.execute(sql, values)		
			self.currHashId = values[0]
		except Exception as err:
			self.error(self.tranCursor, sql, err)


    ## Inserts a new row into the bible_files table
	def replaceFile(self, values):
		sql = ("REPLACE INTO bible_files (hash_id, file_name, book_id, chapter_start, chapter_end,"
			" verse_start, verse_end, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
		try:
			values = (self.currHashId,) + values
			self.sqlLog.write((sql + "\n") % values)
			self.tranCursor.execute(sql, values)
			self.currFileId = self.tranCursor.lastrowid
			self.sqlLog.write("insert bible_files, last insert id %d\n" % self.currFileId)
		except Exception as err:
			self.error(self.tranCursor, sql, err)


	## Inserts a new row into the bible_file_stream_bandwidth table
	def insertBandwidth(self, values):
		sql = ("INSERT INTO bible_file_stream_bandwidths (bible_file_id, file_name, bandwidth, codec, stream)"
			" VALUES (%s, %s, %s, %s, %s)")
		try:
			values = (self.currFileId,) + values + (HLS_CODEC, HLS_STREAM)
			self.sqlLog.write((sql + "\n") % values)
			self.tranCursor.execute(sql, values)
			self.currBandwidthId = self.tranCursor.lastrowid
			self.sqlLog.write("insert bandwidths, last_insert_id %d\n" % self.currBandwidthId)
		except Exception as err:
			self.error(self.tranCursor, sql, err)


	def addSegment(self, values):
		self.segments.append((self.currBandwidthId,) + values)


    ## Inserts a collection of rows into the bible_file_stream_segments table
	def insertSegments(self):
		sql = ("INSERT INTO bible_file_stream_segments (stream_bandwidth_id, timestamp_id, runtime, offset, bytes)"
			+ " VALUES (%s, %s, %s, %s, %s)")
		try:
			for segment in self.segments:
				self.sqlLog.write((sql + "\n") % segment)
			self.tranCursor.executemany(sql, self.segments)
			self.segments = []
		except Exception as err:
			self.error(self.tranCursor, sql, err)


	def commitFilesetInsertTran(self):
		try:
			self.db.commit()
			self.tranCursor.close()
		except Exception as err:
			self.error(self.tranCursor, sql, err)	


	def rollbackFilesetInsertTran(self):
		try:
			self.db.rollback()
			self.tranCursor.close()
		except Exception as err:
			self.error(self.tranCursor, sql, err)		


	def deleteFilesBandwidthsSegments(self, filesetId, bitrate):
		#print(("DELETE %s" % (filesetId)), end="", flush=True)
		cursor = self.tranCursor
		if bitrate == "64":
			bitrateIn = ("48001", "96000")
		elif bitrate == "32":
			bitrateIn = ("24001", "48000")
		elif bitrate == "16":
			bitrateIn = ("0", "24000")
		else:
			self.error(cursor, "DELETE fileset", "Unknown bitrate %s" % (bitrate))
		sql = ("DELETE bfss FROM bible_file_stream_segments AS bfss" 
				" INNER JOIN bible_file_stream_bandwidths AS bfsb ON bfss.stream_bandwidth_id = bfsb.id"
				" INNER JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
				" INNER JOIN bible_filesets AS bs ON bf.hash_id = bs.hash_id"
				" WHERE bs.id = %s AND bfsb.bandwidth BETWEEN %s AND %s")
		cursor.execute(sql, (filesetId, bitrateIn[0], bitrateIn[1]))
		#print("  segments", end="", flush=True)
		sql = ("DELETE bfsb FROM bible_file_stream_bandwidths AS bfsb"
				" INNER JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
				" INNER JOIN bible_filesets AS bs ON bf.hash_id = bs.hash_id"
				" WHERE bs.id = %s AND bfsb.bandwidth BETWEEN %s AND %s")
		cursor.execute(sql, (filesetId, bitrateIn[0], bitrateIn[1]))
		#print("  bandwidths", end="", flush=True)
##
## Convenience Methods
##
	def select(self, statement, values):
		cursor = self.db.cursor()
		try:
			cursor.execute(statement, values)
			resultSet = cursor.fetchall()
			cursor.close()
			return resultSet
		except Exception as err:
			self.error(cursor, statement, err)


	def selectScalar(self, statement, values):
		cursor = self.db.cursor()
		try:
			cursor.execute(statement, values)
			result = cursor.fetchone()
			cursor.close()
			return result[0] if result != None else None
		except Exception as err:
			self.error(cursor, statement, err)


	def selectRow(self, statement, values):
		resultSet = self.select(statement, values)
		return resultSet[0] if len(resultSet) > 0 else [None] * 10	


	def selectList(self, statement, values):
		resultSet = self.select(statement, values)
		results = []
		for row in resultSet:
			results.append(row[0])
		return results


	def error(self, cursor, statement, err):
		if cursor != None:
			cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (err, statement))
		self.db.rollback()
		sys.exit()



hls = AudioHLS()
hls.processCommandLine()
#hls.processBibleId("ENGESV")
#hls.processFilesetId("ENGESV", "ENGESVN2DA")
#hls.close()

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

