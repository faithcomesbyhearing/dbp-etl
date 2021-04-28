# RepairUserDBPRefs.py

from SQLUtility import *

class RepairUserDBPRefs:

	def __init__(self, db):
		self.db = db
		self.updateRows = []


	def repairPlayListItemsFileset(self, dbpName, dbpUserName):
		sql = ("SELECT distinct bs.id, bf.book_id FROM %s.bible_filesets bs"
				" JOIN %s.bible_files bf ON bs.hash_id=bf.hash_id"
				" WHERE bs.set_type_code IN ('audio', 'audio_drama')"
				" AND length(bs.id) <= 12" % (dbpName, dbpName))
		resultSet = self.db.select(sql, ())
		print("num audio filesets and books", len(resultSet))

		filesetMap = {}
		for (filesetId, bookId) in resultSet:
			bookSet = filesetMap.get(filesetId, set())
			bookSet.add(bookId)
			filesetMap[filesetId] = bookSet
		print("num audio fileset", len(filesetMap.keys()))

		sql = ("SELECT id, playlist_id, fileset_id, book_id FROM %s.playlist_items WHERE fileset_id NOT IN"
				" (SELECT id FROM %s.bible_filesets) ORDER BY fileset_id, book_id") % (dbpUserName, dbpName)
		resultSet = self.db.select(sql, ())
		for (playlistItemId, playlistId, filesetId, bookId) in resultSet:
			#print(playlistItemId, playlistId, filesetId, bookId)
			checkFilesetId = filesetId[:6] + "O" + filesetId[7:]
			#print(checkFilesetId)
			found = self._checkPlaylists(filesetMap, checkFilesetId, bookId, playlistItemId)
			if not found:
				checkFilesetId = filesetId[:6] + "N" + filesetId[7:]
				#print(checkFilesetId)
				found = self._checkPlaylists(filesetMap, checkFilesetId, bookId, playlistItemId)
				if not found:
					print("Not Found", filesetId, bookId, playlistId, playlistItemId)
		self._generateUpdate(dbpUserName)


	def _checkPlaylists(self, filesetMap, filesetId, bookId, playlistItemId):
		bookSet = filesetMap.get(filesetId)
		if bookSet == None:
			return False
		if not bookId in bookSet:
			return False
		self.updateRows.append((filesetId, playlistItemId))
		return True


	def _generateUpdate(self, dbpUserName):
		for (filesetId, playlistItemId) in self.updateRows:
			sql = "UPDATE %s.playlist_items SET filesetId = '%s' WHERE id = '%s';" % (dbpUserName, filesetId, playlistItemId)
			print(sql)


if (__name__ == '__main__'):
	from Config import *
	config = Config()
	db = SQLUtility(config)
	repair = RepairUserDBPRefs(db)
	repair.repairPlayListItemsFileset(config.database_db_name, config.database_user_db_name)
	db.close()


"""
SELECT DISTINCT fileset_id
FROM dbp_users.playlist_items
WHERE fileset_id NOT IN (SELECT id FROM dbp_210413.bible_filesets)

SELECT distinct bs.id, bf.book_id FROM bible_filesets bs
JOIN bible_files bf ON bs.hash_id=bf.hash_id
WHERE bs.set_type_code like 'audio%'
AND length(bs.id) <= 12
"""
