# CreateDAFileset

# This program creates additional DA filesets to enable testing so
# that there are multiple filesets present for AudioHLS

import hashlib
from SQLUtility import *

class CreateDAFileset:

	def __init__(self):
		self.db = SQLUtility("localhost", 3306, "root", "hls_dbp")

	def process(self, filesetId, bitrate):
		resultSet = self.db.select("SELECT hash_id, asset_id, set_type_code, set_size_code, hidden"
			" FROM bible_filesets WHERE id = %s", (filesetId))
		row = resultSet[0]
		hashId = row[0]
		assetId = row[1]
		setTypeCode = row[2]
		setSizeCode = row[3]
		hidden = row[4]
		newFilesetId = filesetId + bitrate
		newHashId = self.getHashId(assetId, newFilesetId, setTypeCode)
		sql = ("INSERT INTO bible_filesets (id, hash_id, asset_id, set_type_code, set_size_code, hidden)"
					" VALUES (%s, %s, %s, %s, %s, %s)")
		self.db.execute(sql, (newFilesetId, newHashId, assetId, setTypeCode, setSizeCode, hidden))

		sql = ("INSERT INTO bible_files (hash_id, book_id, chapter_start, chapter_end, verse_start, verse_end,"
				" file_name, file_size, duration)"
				" SELECT %s, book_id, chapter_start, chapter_end, verse_start, verse_end,"
				" file_name, file_size, duration FROM bible_files WHERE hash_id = %s")
		self.db.execute(sql, (newHashId, hashId))


	def getHashId(self, bucket, filesetId, setTypeCode):
		md5 = hashlib.md5()
		md5.update(filesetId.encode("latin1"))
		md5.update(bucket.encode("latin1"))
		md5.update(setTypeCode.encode("latin1"))
		hash_id = md5.hexdigest()
		return hash_id[:12]


test = CreateDAFileset()
test.process("ENGESVN2DA", "48")
