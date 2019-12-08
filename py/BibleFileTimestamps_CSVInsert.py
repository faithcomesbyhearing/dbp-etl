# BibleFileTimestamps_CSVInsert.py
#
# This program loads timings data into the bible_file_timestamps table.
# It reads one CSV file that contains the timings, and inserts the contents as one batch.
#
# Usage: python3 py/BibleFileTimestamps_CSVInsert.py  /top/dir/filename.csv timing_est_err
# Sample Quality codes:
# 0 = verified by known native speaker
# 1 = verified by unknown native speaker
# 2 = verified by non-native speaker
# 3 = verified by non-speaker (eg members of the text team at FCBH who do this type of thing for a living)
# 5 = automated spot-checked by non-native speaker (this is the case of most of the SAB appDef timings)
# 9 = automated unverified (probably would never put these into the DB, but if I did at least I’d have this indicator of non-confidence)

import sys
import io
import csv
from SQLUtility import *


TIM_HOST = "localhost"
TIM_USER = "root"
TIM_PORT = 3306
TIM_DB_NAME = "dbp"


class BibleFileTimestamps_CSVInsert:

	def __init__(self):
		self.db = SQLUtility(TIM_HOST, TIM_PORT, TIM_USER, TIM_DB_NAME)
		self.bookCodeMap = self.db.selectMap("SELECT id_osis, id FROM books", ())


	def getCommandLine(self):
		if len(sys.argv) < 3:
			print("Usage: python3 py/BibleFileTimestamps_CSVInsert.py  /top/dir/filename.csv timing_est_err")
			sys.exit()
		path = sys.argv[1]
		if not os.path.exists(path):
			print("ERROR: File to load %s does not exist" % (path,))
			sys.exit()
		qualityCode = sys.argv[2]
		if not qualityCode.isdigit():
			print("ERROR: Quality code %s must be numeric" % (qualityCode))
			sys.exit()
		return (path, qualityCode)


	def getFilesetIdList(self, path):
		filesetIdSet = set()
		with open(path, 'rt') as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				if row[1] != "dam_id":
					filesetIdSet.add(row[1])
		return list(filesetIdSet)


	def getFileIdMap(self, filesetIdList):
		sql = ("SELECT concat(bs.id, ':', bf.book_id, ':', bf.chapter_start), bf.id FROM bible_files bf"
			" INNER JOIN bible_filesets bs ON bf.hash_id = bs.hash_id"
			" WHERE bs.set_type_code IN ('audio', 'audio_drama')"
			" AND bs.id IN ") + "(" + filesetIdList + ")"
		return self.db.selectMap(sql, ())


	def getHashIdMap(self, filesetIdList):
		sql = ("SELECT id, hash_id FROM bible_filesets WHERE asset_id = 'dbp-prod'"
			" AND set_type_code IN ('audio', 'audio_drama')"
			" AND id IN ") + "(" + filesetIdList + ")"
		return self.db.selectMap(sql, ())


	def process(self):
		(path, qualityCode) = self.getCommandLine()
		filesetIdList = self.getFilesetIdList(path)
		filesetIdStr = "'" + "','".join(filesetIdList) + "'"
		fileIdMap = self.getFileIdMap(filesetIdStr)
		hashIdMap = self.getHashIdMap(filesetIdStr)
		values = []
		with open(path, 'rt') as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in reader:
				if row[1] != "dam_id":
					filesetId = row[1]
					osisCode = row[2]
					bookId = self.bookCodeMap.get(osisCode)
					if bookId == None:
						print("ERROR: Book code %s in line %s is not known" % (osisCode, row[0]))
						sys.exit()
					chapter = row[3]
					verse = row[4]
					timing = round(float(row[5]), 2)
					key = "%s:%s:%s" % (filesetId, bookId, chapter)
					fileId = fileIdMap.get(key)
					if fileId == None:
						print("ERROR: fileId is not found for %s" % (key))
						sys.exit()
					values.append((fileId, verse, None, timing))#, row))

		cursor = self.db.conn.cursor()
		print("Delete timestamps for filesets", filesetIdList)
		print("Row %d to be inserted" % (len(values),))
		deleteStmt = ("DELETE FROM bible_file_timestamps WHERE bible_file_id IN"
			" (SELECT bf.id FROM bible_files bf, bible_filesets bs"
			" WHERE bf.hash_id = bs.hash_id AND bs.id = %s)")
		errorStmt = ("REPLACE INTO bible_fileset_tags (hash_id, name, description, admin_only, iso, language_id)"
			" VALUES (%s, 'timing_est_err', %s, 0, 'eng', 6414)")
		insertStmt = "INSERT INTO bible_file_timestamps (bible_file_id, verse_start, verse_end, `timestamp`) VALUES (%s, %s, %s, %s)"
		#for value in sorted(values):
		#	print(value)
		#print(len(values))
		try:
			for filesetId in filesetIdList:
				sql = deleteStmt
				cursor.execute(sql, (filesetId,))
				hashId = hashIdMap[filesetId] # intend to crash if not found
				sql = errorStmt
				cursor.execute(sql, (hashId, qualityCode))
				sql = insertStmt
			cursor.executemany(sql, values)
			self.db.conn.commit()
			cursor.close()
		except Exception as err:
			self.db.error(cursor, sql, err)

		self.db.close()


ins = BibleFileTimestamps_CSVInsert()
ins.process()


