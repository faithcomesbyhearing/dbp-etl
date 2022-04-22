#!/usr/bin/python3
#
# ValidatePage.py
#
# This program presents an information page that can be used
# to validate the correctness of information about a Bible.
# It displays information about that Bible, and reports
# any errors that it finds.
#
# This class should be in the cgi-bin directory of a website.
# Where it is executed
#
# Requires a query string 
# MYSQL_CONFIG=host:localhost,user:root,password:***,db:dbp,port:3306,output:html
# MYSQL_CONFIG=host:127.0.0.1,user:sa,password:***,db:dbp_NEWDATA,port:3306,output:html



import os
import tempfile
import sys
import pymysql
from OutputHTML import *

class ValidatePage:
	
	def parseQuery():
		query = os.environ["QUERY_STRING"]
		if query != None and len(query) > 0 and "=" in query:
			parts = query.split("=")
			if parts[0].lower() == "bibleid":
				return (parts[1], None)
			if parts[0].lower() == "filesetid":
				return (None, parts[1])
		return (None, None)
			
			
	def parseCommand():
		if len(sys.argv) > 1:
			return (None, sys.argv[1])
		return None
			
	
	def __init__(self):
		map = {}
		config = os.environ.get("MYSQL_CONFIG")
		if config != None and "," in config:
			parts = config.split(",")
			for part in parts:
				if ":" in part:
					pieces = part.split(":", 2)
					map[pieces[0]] = pieces[1]
		self.format = map.get("output")
		self.output = OutputHTML()
		try:
			self.conn = pymysql.connect(host = map.get("host"),
	                             		user = map.get("user"),
	                             		password = map.get("password"),
	                             		db = map.get("db"),
	                             		port = int(map.get("port")),
	                             		charset = 'utf8mb4',
	                             		cursorclass = pymysql.cursors.Cursor)
			print("Database '%s' is opened." % (map.get("db")))
		except Exception as err:
			print("ERROR opening Database. MYSQL_CONFIG=%s" % (config), err)
			sys.exit()
			
	def close(self):
		if self.conn != None:
			self.conn.close()
			self.conn = None
		self.output.file(tempfile.gettempdir() + "/sample.html")
		print("file written to %s" % (tempfile.gettempdir() + "/sample.html"))
			
	def getBibleId(self, filesetId):
		resultSet = self.query("SELECT bible_id FROM bibles_view WHERE fileset_id = %s", (filesetId))
		bibleId = resultSet[0][0]
		#print("filesetId [%s] yields bibleId [%s]" % (filesetId, bibleId))
		return bibleId

	def getHashIds(self, filesetId):
		resultSet = self.query("SELECT hash_id FROM bible_filesets WHERE id = %s", (filesetId))
		results = []
		for row in resultSet:
			results.append(row[0])
		hashIdList = "'" + "','".join(results) + "'"	
		print(hashIdList)
		return hashIdList
		
	def process(self, filesetId):	
		bibleId = self.getBibleId(filesetId)
		hashIdList = self.getHashIds(filesetId)

		self.displayQuery("Bibles", 
			["bible_id", "language_id", "versification", "numeral_system_id", "date", "scope", "script", "derived", "copyright", "priority", "reviewed", 
			"notes", "created_at", "updated_at"],
			("SELECT id, language_id, versification, numeral_system_id, date, scope, script, derived, copyright, priority, reviewed," +
				" notes, created_at, updated_at FROM bibles WHERE id = %s"), (bibleId,))
		self.displayQuery("Bible Language", 
			["bible_id", "language_id", "language_name", "iso", "glotto_id", "area", "population", "writing", "country_id", "created_at", "updated_at"],
			("SELECT b.id, language_id, name, iso, glotto_id, area, population, writing, country_id, l.created_at, l.updated_at " + 
				" FROM bibles b join languages l on l.id = b.language_id " + 
				"WHERE b.id = %s"), (bibleId,))
		
		self.displayQuery("Language Translation", 
			["bible_id", "language_source_id", "language_source_name", "language_translation_id", "translation_name", "priority", "created_at", "updated_at"],
			("SELECT b.id, lt.language_source_id, l.name as 'language_name', lt.language_translation_id, lt.name as 'translation name', lt.priority, lt.created_at, lt.updated_at " + 
				" FROM bibles b join languages l on l.id = b.language_id join language_translations lt on lt.language_source_id = l.id" + 
				" WHERE b.id = %s and lt.priority=9"), (bibleId,))

		self.displayQuery("Bible Translation", 
			["id", "language_id", "bible_id", "vernacular", "vernacular_trade", "name", "created_at", "updated_at"],
			("select id, language_id, bible_id, vernacular, vernacular_trade, name, bt.created_at, bt.updated_at " + 
				" from bible_translations bt " + 
				" WHERE bt.bible_id = %s"), (bibleId,))

# select id, language_id, bible_id, vernacular, vernacular_trade, name, bt.created_at, bt.updated_at
# from bible_translations bt
# where bt.bible_id = 'KMLWBT'
		self.displayQuery("Bible_Filesets",
			["fileset_id", "hash_id", "asset_id", "set_type_code", "set_size_code", "hidden", "created_at", "updated_at"],
			("SELECT f.id, f.hash_id, f.asset_id, f.set_type_code, f.set_size_code, f.hidden, f.created_at, f.updated_at" +
				" FROM bible_filesets f WHERE hash_id IN (%s) ORDER BY f.id" % (hashIdList,)), ())
				
		self.displayQuery("Access_Group_Filesets",
			["fileset_id", "access_group_id", "group_name", "created_at", "updated_at"],
			("SELECT distinct f.id, a.access_group_id, t.name, a.created_at, a.updated_at" +
				" FROM access_group_filesets a JOIN bible_filesets f ON a.hash_id = f.hash_id" +
				" JOIN access_groups t ON a.access_group_id = t.id" +
				" WHERE a.hash_id IN (%s) ORDER BY f.id, a.access_group_id" % (hashIdList,)), ())
				
		self.displayQuery("Bible_Fileset_Copyright_Organizations",
			["fileset_id", "organization_id", "organization_slug", "organization_role", "created_at", "updated_at"],
			("SELECT distinct s.id, f.organization_id, o.slug, f.organization_role, f.created_at, f.updated_at" +
				" FROM bible_fileset_copyright_organizations f" +
				" JOIN bible_filesets s ON f.hash_id = s.hash_id" +
				" JOIN organizations o ON o.id = f.organization_id" +
				" WHERE f.hash_id IN (%s) ORDER BY s.id" % (hashIdList,)), ())
				
		self.displayQuery("Bible_Fileset_Copyrights", 
			["fileset_id", "copyright_date", "copyright", "copyright_description", "created_at", "updated_at"],
			("SELECT distinct s.id, f.copyright_date, f.copyright, f.copyright_description, f.created_at, f.updated_at" + 
				" FROM bible_fileset_copyrights f" +
				" JOIN bible_filesets s ON f.hash_id = s.hash_id" +
				" WHERE f.hash_id IN (%s) ORDER by s.id" % (hashIdList,)), ())
				
		self.displayQuery("Bible_Fileset_Tags",
			["fileset_id", "name", "description", "created_at", "updated_at"],
			("SELECT s.id, f.name, f.description, f.created_at, f.updated_at" +
				" FROM bible_fileset_tags f" +
				" JOIN bible_filesets s ON s.hash_id = f.hash_id" +
				" WHERE f.hash_id IN (%s) ORDER by s.id" % (hashIdList,)), ())
				
		self.displayQuery("Bible_Fileset_Fonts",
			["fileset_id", "font_id", "created_at", "updated_at"],
			("SELECT s.id, f.font_id, f.created_at, f.updated_at" +
				" FROM bible_fileset_fonts f" +
				" JOIN bible_filesets s ON s.hash_id = f.hash_id" +
				" WHERE f.hash_id IN (%s) ORDER BY s.id" % (hashIdList,)), ())
		
		self.displayQuery("Bible_File_Tags",
			["fileset_id", "file_name", "tag", "value", "created_at", "updated_at"],
			("SELECT s.id, bf.file_name, f.tag, f.value, f.admin_only, f.created_at, f.updated_at" +
				" FROM bible_file_tags f" +
				" JOIN bible_files bf ON f.file_id = bf.id" +
				" JOIN bible_filesets s ON s.hash_id = bf.hash_id" +
				" WHERE bf.hash_id IN (%s) ORDER BY s.id" % (hashIdList,)), ())
			
		""""		
		self.displayQuery("Bible_Files",
			["fileset_id", "file_id", "book_id", "chapter_start", "chapter_end", "verse_start", "verse_end", "file_name", "file_size", "duration",
			 "created_at, updated_at"],
			 ("SELECT s.id, f.id, f.book_id, f.chapter_start, f.chapter_end, f.verse_start, f.verse_end, f.file_name, f.file_size, f.duration," +
			 	" f.created_at, f.updated_at" +
			 	" FROM bible_files f" +
			 	" JOIN bible_filesets s ON s.hash_id = f.hash_id" +
			 	" WHERE f.hash_id IN (%s) ORDER BY s.id, f.book_id, f.chapter_start, f.verse_start" % (hashIdList,)), ())
		"""	 
			
		self.displayQuery("Bible_Files_Secondary",
			["fileset_id", "file_name", "file_type", "created_at", "updated_at"],
			("SELECT s.id, f.file_name, f.file_type, f.created_at, f.updated_at" +
			" FROM bible_files_secondary f" +
			" JOIN bible_filesets s ON s.hash_id = f.hash_id" +
			" WHERE f.hash_id IN (%s) ORDER BY s.id, f.file_name" % (hashIdList,)), ())
			
		self.displayQuery("Bible_File_Titles",
			["fileset_id", "file_name", "iso", "title", "description", "key_words", "created_at", "updated_at"],
			("SELECT s.id, bf.file_name, f.iso, f.title, f.description, f.key_words, f.created_at, f.updated_at" +
				" FROM bible_file_titles f" +
				" JOIN bible_files bf ON bf.id = f.file_id" +
				" JOIN bible_filesets s ON s.hash_id = bf.hash_id" +
				" WHERE bf.hash_id IN (%s) ORDER BY s.id, bf.file_name" % (hashIdList,)), ())

		
		
	def displayQuery(self, title, columns, statement, values):
		resultSet = self.query(statement, values)
		#print(resultSet)
		if self.format == "html":
			self.output.table(title, columns, resultSet)
		
	def query(self, statement, values):
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			resultSet = cursor.fetchall()
			cursor.close()
			return resultSet
		except Exception as error:
			cursor.close()	
			print("ERROR executing SQL %s on '%s'" % (error, statement))
			self.conn.rollback()
			sys.exit()
		
			
if __name__ == "__main__":
	filesetId = "ENGESVN1DA"
	page = ValidatePage()
	page.process(filesetId)
	page.close()

# python3 load/ValidatePage.py 
	
"""
	
bible_file_stream_bandwidths
	fileset_id, file_name, file_name, bandwidth, resolution_width, resolution_height, codec, stream, created_at, updated_at
SELECT s.id, bf.file_name, f.file_name, f.bandwidth, f.resolution_width, f.resolution_height, f.codec, f.stream, f.created_at, f.updated_at
FROM bible_file_stream_bandwidths f 
JOIN bible_files bf ON f.bible_file_id = bf.id
JOIN bible_fileset_connections c ON c.hash_id = bf.hash_id
JOIN bible_filesets s ON s.hash_id = bf.hash_id
WHERE c.bible_id = 'ENGESV';

bible_file_stream_bytes
	fileset_id, file_name, runtime, bytes, offset, timestamp_id, created_at, updated_at
SELECT s.id, b.file_name, f.runtime, f.bytes, f.offset, f.timestamp_id, f.created_at, f.updated_at
FROM bible_file_stream_bytes f 
JOIN bible_file_stream_bandwidths b ON f.stream_bandwidth_id = b.id
JOIN bible_files bf ON b.bible_file_id = bf.id
JOIN bible_fileset_connections c ON c.hash_id = bf.hash_id
JOIN bible_filesets s ON s.hash_id = bf.hash_id
WHERE c.bible_id = 'ENGESV'

bible_file_stream_ts
	fileset_id, file_name, file_name, runtime, created_at, updated_at
SELECT s.id, b.file_name, f.file_name, f.runtime, f.created_at, f.updated_at
FROM bible_file_stream_ts f 
JOIN bible_file_stream_bandwidths b ON f.stream_bandwidth_id = b.id
JOIN bible_files bf ON b.bible_file_id = bf.id
JOIN bible_fileset_connections c ON c.hash_id = bf.hash_id
JOIN bible_filesets s ON s.hash_id = bf.hash_id
WHERE c.bible_id = 'ENGESV'

bible_books
	bible_id, book_id, name, name_short, chapters, created_at, updated_at, book_seq
SELECT bible_id, book_id, name, name_short, chapters, created_at, updated_at, book_seq
FROM bible_books
WHERE bible_id = 'ENGESV';

bible_equivalents
	bible_id, equivalent_id, organization_id, type, site, suffix, created_at, updated_at
SELECT bible_id, equivalent_id, organization_id, type, site, suffix, created_at, updated_at
FROM bible_equivalents
WHERE bible_id = 'ENGESV';

bible_verses
	show count, using hash_id
SELECT hash_id, count(*) 
FROM bible_verses
GROUP BY hash_id
SELECT count(*)
FROM bible_verses f
JOIN bible_fileset_connections c ON c.hash_id = f.hash_id
	
bible_file_timestamps;
	count only, using bible_file_id

run_history
	run_id, run_time, username, location, directory, errors
	display recent only
SELECT run_id, run_time, username, location, directory, errors
FROM run_history
WHERE run_time > now() - interval 4 month;

run_batch
	run_id, batch, status
	display those in run_history display
SELECT run_id, batch, status
FROM run_batch
WHERE run_id IN (SELECT run_id FROM run_history WHERE run_time > now() - interval 4 month)
	
"""	
	
	
	