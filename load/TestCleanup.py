# TestCleanup.py

# This standalone program is intended to delete all of the data related to a fileset
# or a bible_book, so that the update logic of fileset and bible_book can be tested.

import sys
from Config import *
from SQLUtility import *

def deleteFileset(db, filesetId):
	hashId = db.selectScalar("SELECT hash_id FROM bible_filesets WHERE id=%s", (filesetId,))
	bibleId = db.selectScalar("SELECT bible_id FROM bible_fileset_connections WHERE hash_id=%s", (hashId,))
	sql = []
	sql.append("DELETE bfss FROM bible_file_stream_ts AS bfss"
		" JOIN bible_file_stream_bandwidths AS bfsb ON bfss.stream_bandwidth_id = bfsb.id"
		" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
		" WHERE bf.hash_id = %s")
	sql.append("DELETE bfsb FROM bible_file_stream_bandwidths AS bfsb"
		" JOIN bible_files AS bf ON bfsb.bible_file_id = bf.id"
		" WHERE bf.hash_id = %s")
	sql.append("DELETE bft FROM bible_file_tags AS bft"
		" JOIN bible_files AS bf ON bft.file_id = bf.id"
		" WHERE bf.hash_id = %s")
	sql.append("DELETE FROM bible_files_secondary WHERE hash_id = %s")
	sql.append("DELETE FROM bible_files WHERE hash_id = %s")
	sql.append("DELETE FROM bible_verses WHERE hash_id = %s")
	sql.append("DELETE FROM access_group_filesets WHERE hash_id = %s")
	sql.append("DELETE FROM bible_fileset_connections WHERE hash_id = %s")
	sql.append("DELETE FROM bible_fileset_tags WHERE hash_id = %s")
	sql.append("DELETE FROM bible_fileset_copyright_organizations WHERE hash_id = %s")
	sql.append("DELETE FROM bible_fileset_copyrights WHERE hash_id = %s")
	sql.append("DELETE FROM bible_filesets WHERE hash_id = %s")
	for stmt in sql:
		db.execute(stmt, (hashId,))
	db.execute("DELETE FROM bible_books WHERE bible_id = %s", (bibleId,))


if (__name__ == '__main__'):
	if len(sys.argv) != 3:
		print("Usage load/TestCleanup.py  config_profile  filesetId_to_delete")
	else:
		filesetId = sys.argv[2]
		print("filesetId", filesetId)
		config = Config()
		db = SQLUtility(config)
		deleteFileset(db, filesetId)

