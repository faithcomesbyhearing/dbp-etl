# AudioHLSAdapter


from SQLUtility import *

HOST = "localhost"
USER = "root"
PORT = 3306
DB_NAME = "hls_dbp"
CODEC = "avc1.4d001f,mp4a.40.2"
STREAM = "1"

# insert into bible_fileset_types (set_type_code, name) values ('audio_stream','HLS Audio Stream');

class AudioHLSAdapter:

	def __init__(self):
		self.db = SQLUtility(HOST, PORT, USER, DB_NAME)
		self.cursor = self.db.conn.cursor()
		self.currHashId = None
		self.currFileId = None
		self.currBandwidthId = None
		self.segments = []

	def close(self):
		self.cursor.close()
		self.db.close()


	## Query finds audio filesets without HLS where timestamp data is available
	## Limitation: It cannot distinquish between a fileset that has a complete HLS and one with a little
	## This is not being used
	def findFilesetIdNeedHLS(self):
		sql = ("SELECT DISTINCT bs.id"
			" FROM bible_file_timestamps bft, bible_files bf, bible_filesets bs"
			" WHERE bft.bible_file_id = bf.id AND bf.hash_id = bs.hash_id"
			" AND bs.id NOT IN (SELECT CONCAT(LEFT(id,8),'DA',MID(id,11,2))"
			" FROM bible_filesets WHERE set_type_code = 'audio_stream')")
		return self.db.selectList(sql, None)


	def checkBibleId(self, bibleId):
		sql = "SELECT count(*) FROM bible_fileset_connections WHERE bible_id=%s"
		return self.db.selectScalar(sql, (bibleId,))


	def checkFilesetId(self, bibleId, filesetId):
		sql = ("SELECT count(*) FROM bible_fileset_connections bfc, bible_filesets bf"
			" WHERE bfc.bible_id=%s AND bf.id=%s")
		return self.db.selectScalar(sql, (bibleId, filesetId))


	def checkBibleTimestamps(self, bibleId):
		sql = ("SELECT count(*) FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs,"
			" bible_fileset_connections bfc"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id AND bfc.hash_id=bfs.hash_id"
			" AND bfc.bible_id=%s")
		return self.db.selectScalar(sql, (bibleId,))


	## Returns the number of rows of timestamp data available for a filesetId
	def checkFilesetTimestamps(self, filesetId):
		sql = ("SELECT count(*) FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id AND bfs.id=%s")
		return self.db.selectScalar(sql, (filesetId,))


	## Returns the audio fileset_ids for a bible_id
	def selectFilesetIds(self, bibleId):
		sql = ("SELECT bf.id FROM bible_filesets bf, bible_fileset_connections bfc"
			" WHERE bf.hash_id=bfc.hash_id AND bfc.bible_id=%s"
			" AND bf.set_type_code IN ('audio', 'audio_drama')")
		return self.db.selectList(sql, (bibleId,))


	def selectFileset(self, filesetId):
		sql = "SELECT asset_id, set_type_code, set_size_code FROM bible_filesets WHERE id=%s"
		return self.db.selectRow(sql, (filesetId,))


    ## Returns the files associated with a fileset
	def selectBibleFiles(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, bf.chapter_end, bf.verse_start," 
			" bf.verse_end, bf.file_size, bf.duration"
			" FROM bible_files bf, bible_filesets bfs"
			" WHERE bf.hash_id=bfs.hash_id AND bfs.id=%s"
			" ORDER BY bf.file_name")
		return self.db.select(sql, (filesetId,))


    ## Returns the timestamps of a fileset in a map[book:chapter:verse] = timestamp
	def selectTimestamps(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, ft.id, ft.verse_start, ft.timestamp"
			" FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id"
			" AND bfs.id=%s ORDER BY bf.file_name, ft.verse_start")
		resultSet = self.db.select(sql, (filesetId,))
		results = {}
		for row in resultSet:
			key = "%s:%d" % (row[1], row[2]) # book:chapter
			times = results.get(key, [])
			times.append((row[3], row[5]))
			results[key] = times
		return results


	def beginFilesetInsertTran(self):
		try:
			#self.cursor.execute("BEGIN")
			self.db.conn.begin()
		except Exception as err:
			self.error(self.cursor, sql, err)


    ## Inserts a new row into the fileset table
	def insertFileset(self, values):
		sql = ("INSERT INTO bible_filesets (hash_id, id, asset_id, set_type_code, set_size_code)"
			" VALUES (%s, %s, %s, %s, %s)")
		try:
			print(sql % values)
			self.cursor.execute(sql, values)		
			self.currHashId = values[0]

			self.db.conn.commit()
		except Exception as err:
			self.error(self.cursor, sql, err)


    ## Inserts a new row into the bible_files table
	def insertFile(self, values):
		sql = ("INSERT INTO bible_files (hash_id, file_name, book_id, chapter_start, chapter_end,"
			" verse_start, verse_end, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
		try:
			values = (self.currHashId,) + values
			print(sql % values)
			self.cursor.execute(sql, values)
			self.currFileId = self.cursor.lastrowid
			print("insert bible_files, last insert id", self.currFileId)
		except Exception as err:
			self.error(self.cursor, sql, err)


	## Inserts a new row into the bible_file_stream_bandwidth table
	def insertBandwidth(self, values):
		sql = ("INSERT INTO bible_file_stream_bandwidths (bible_file_id, file_name, bandwidth, codec, stream)"
			" VALUES (%s, %s, %s, %s, %s)")
		try:
			values = (self.currFileId,) + values + (CODEC, STREAM)
			print(sql % values)
			self.cursor.execute(sql, values)
			self.currBandwidthId = self.cursor.lastrowid
			print("insert bandwidths, last_insert_id", self.currBandwidthId)
		except Exception as err:
			self.error(self.cursor, sql, err)


	def addSegment(self, values):
		self.segments.append((self.currBandwidthId,) + values)


    ## Inserts a collection of rows into the bible_file_stream_segments table
	def insertSegments(self):
		sql = ("INSERT INTO bible_file_stream_segments (stream_bandwidth_id, timestamp_id, runtime, offset, bytes)"
			+ " VALUES (%s, %s, %s, %s, %s)")
		try:
			for segment in self.segments:
				print(sql % segment)
			self.cursor.executemany(sql, self.segments)
			self.segments = []
		except Exception as err:
			self.error(self.cursor, sql, err)


	def commitFilesetInsertTran(self):
		try:
			self.db.conn.commit()
		except Exception as err:
			self.error(self.cursor, sql, err)			


	## Test after new HLS data added, performance could probably be improved by changing to joins
	def deleteFileset(self, filesetId):
		self.cursor.execute("BEGIN")
		sql = ("DELETE FROM bible_file_stream_segments WHERE stream_bandwidth_id IN"
				+ "(SELECT id FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
				+ "(SELECT id FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s)))")
		self.cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_file_stream_bandwidths WHERE bible_file_id IN"
				+ "(SELECT id FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s))")
		self.cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s)")
		self.cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_filesets WHERE id=%s")
		self.cursor.execute(sql, (filesetId,))
		self.db.conn.commit()


	def error(self, cursor, statement, err):
		self.cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (err, statement))
		self.db.conn.rollback()
		sys.exit()


