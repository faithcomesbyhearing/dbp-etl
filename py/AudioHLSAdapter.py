# AudioHLSAdapter


from SQLUtility import *

HOST = "localhost"
USER = "root"
PORT = 3306
DB_NAME = "dbp_hls"
ASSET_ID = "dbp-prod"
TYPE_CODE = "audio_stream"
CODEC = "avc1.4d001f,mp4a.40.2"
STREAM = "1"

class AudioHLSAdapter:

	def __init__(self):
		self.db = SQLUtility(HOST, USER, PORT, DB_NAME)
		self.cursor = self.db.cursor()
		self.currHashId = None
		self.currFileId = None
		self.currBandwidthId = None
		self.segments = []

	def close(self):
		self.cursor.close() # test closing twice
		self.db.close() # test closing twice


	## Returns the number of rows of timestamp data available for a filesetId
	def hasTimestamps(self, filesetId):
		sql = ("SELECT count(*) FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			" WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id AND bfs.id=%s")
		return self.db.selectScalar(sql, (filesetId,))


	## Returns the audio fileset_id for a bible_id
	def selectFilesets(self, bibleId):
		sql = ("SELECT bf.id FROM bible_filesets bf, bible_fileset_connections bfc"
			+ " WHERE bf.hash_id=bfc.hash_id AND bfc.bible_id=%s"
			+ " AND bf.set_type_code IN ('audio', 'audio_drama')")
		return self.db.selectList(sql, (bibleId,))


    ## Returns the files associated with a fileset
	def selectBibleFiles(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, bf.chapter_end, bf.verse_start, bf.verse_end"
			# I think that I don't need file_size and duration
			+ " FROM bible_files bf, bible_filesets bfs"
			+ " WHERE bf.hash_id=bfs.hash_id AND bfs.id=%s"
			+ " ORDER BY bf.file_name")
		return self.db.select(sql, (filesetId,))


    ## Returns the timestamps of a fileset in a map[book:chapter:verse] = timestamp
	def selectTimestamps(self, filesetId):
		sql = ("SELECT bf.file_name, bf.book_id, bf.chapter_start, ft.verse_start, ft.timestamp"
			+ " FROM bible_file_timestamps ft, bible_files bf, bible_filesets bfs"
			+ " WHERE bf.id=ft.bible_file_id AND bfs.hash_id=bf.hash_id"
			+ " AND bfs.id=%s ORDER BY bf.file_name, ft.verse_start")
		resultSet = self.db.select(sql, (filesetId,))
		results = {}
		for row in resultsSet:
			key = "%s:%d:%d" % (row[1], row[2], row[3]) # book:chapter:verse
			results[key] = row[4] # timestamp
		return results


    ## Inserts a new row into the fileset table
	def insertFileset(self, filesetId, hashId, setSizeCode): # I either need to add assetId or remove hashId
		try:
			self.cursor.execute("BEGIN")
			sql = ("INSERT INTO bible_fileset (hash_id, id, asset_id, set_type_code, set_size_code)"
				+ " VALUES (%s, %s, %s, %s, %s)")
			values = (hashId, filesetId, ASSET_ID, TYPE_CODE, setSizeCode)
			self.cursor.execute(sql, values)			
			self.currHashId = hashId
		except Exception as err:
			self.error(cursor, statement, err)


    ## Inserts a new row into the bible_files table
	def insertFile(self, bookId, chapterStart, chapterEnd, verseStart, verseEnd, filename, fileSize, duration):
		try:
			sql = ("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start,"
				+ " verse_end, file_name, file_size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
			values = (self.currHashId, bookId, chapterStart, chapterEnd, verseStart, verseEnd, 
					filename, fileSize, duration)
			self.cursor.execute(sql, values)
			self.currFileId = self.cursor.execute("LAST_INSERT()")
		except Exception as err:
			self.error(cursor, statement, err)


	## Inserts a new row into the bible_file_stream_bandwidth table
	def insertBandwidth(self, fileId, filename, bandwidth, width, height):
		try:
			sql = ("INSERT INTO bible_file_stream_bandwidths (file_id, file_name, bandwidth, resolution_width,"
				+ " resolution_height, codec, stream) VALUES (%s, %s, %s, %s, %s, %s, %s)")
			values = (self.currFileId, filename, bandwidth, width, height, CODEC, STREAM)
			self.cursor.execute(sql, values)
			self.currBandwidthId = self.cursor.execute("LAST_INSERT()")
		except Exception as err:
			self.error(cursor, statement, err)


	def addSegment(self, timestampId, byteCount, offset, duration):
		self.segments.append((self.currBandwidthId, timestampId, byteCount, offset, duration))


    ## Inserts a collection of rows into the bible_file_stream_segments table
	def insertSegments(self):
		try:
			sql = ("INSERT INTO bible_file_stream_segments (bandwidthId, timestampId, byteCount, offset, duration)"
				+ " VALUES (%s, %s, %s, %s, %s)")
			cursor.executemany(statement, self.segments)
			self.db.commit()
		except Exception as err:
			self.error(cursor, statement, err)


	## Test after new HLS data added, performance could probably be improved by changing to joins
	def deleteFileset(self, filesetId):
		cursor.execute("BEGIN")
		sql = ("DELETE FROM bible_file_stream_segments WHERE bandwidth_id IN"
				+ "(SELECT id FROM bible_file_stream_bandwidths WHERE file_id IN"
				+ "(SELECT id FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s)))")
		cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_file_stream_bandwidths WHERE file_id IN"
				+ "(SELECT id FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s))")
		cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_files WHERE hash_id IN"
				+ "(SELECT hash_id FROM bible_filesets WHERE id=%s))")
		cursor.execute(sql, (filesetId,))
		sql = ("DELETE FROM bible_filesets WHERE id=%s)")
		cursor.execute(sql, (filesetId,))
		self.db.commit()


	def error(self, cursor, statement, err):
		cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (error, stmt))
		self.db.rollback()
		sys.exit()


