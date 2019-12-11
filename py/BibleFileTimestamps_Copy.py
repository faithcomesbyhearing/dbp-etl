# BibleFileTimestamps_Copy.py

# This program copies bible_file_timestamp data between sister filesets (eg N1 and N2 for a given bible), 
# if the corresponding chapter durations are within some delta (eg a second by default, 
# but setable via command line argument).
#
# Usage: python3 py/BibleFileTimings_Copy.py starting_bible_id ending_bible_id src_timing_err_est, duration_err_limit
#
# 1. Read in four parameters, starting_bible_id, ending_bible_id, src_timing_err_est, duration_err_limit
# 2. Iterate over bible_id's, find group of fileset_ids for each bible_id
# 3. Find all fileset_id that have no timing_est_err, and create list of possible targets
# 4. Find the OT fileset and the NT fileset with the best timing_err_est
# 5. Iterate over each target, and each source within that target
# 6. Compute duration differences between source.
# 7. If there is a count, and median difference is within paramters
# 8. Then perform copy, and update timing_id and timing_est_err

# Note: This solution is going to redo a copy that has already been done using the best quality
# timestamps available at that time.

# Required for the program to increase size of bible_file_tags.tag
# alter table bible_file_tags modify tag varchar(16) not null;


import sys
import os
import io
import statistics
from SQLUtility import *


DUR_HOST = "localhost"
DUR_USER = "root"
DUR_PORT = 3306
DUR_DB_NAME = "dbp"


class BibleFileTimestamps_Copy:

	def __init__(self):
		self.db = SQLUtility(DUR_HOST, DUR_PORT, DUR_USER, DUR_DB_NAME)


	def getCommandLine(self):
		if len(sys.argv) < 5:
			print("Usage: BibleFileTimestamps_Copy.py starting_bible_id ending_bible_id src_timing_err_est duration_err_limit")
			sys.exit()
		startingBibleId = sys.argv[1]
		endingBibleId = sys.argv[2]
		srcTimingErrEst = sys.argv[3]
		durationErrLimit = sys.argv[4]
		if not srcTimingErrEst.isdigit():
			print("ERROR: src_timing_err_est %s must be numeric" % (srcTimingErrEst))
			sys.exit()
		if not durationErrLimit.isdigit():
			print("ERROR: duration_err_limit %s must be numeric" % (durationErrLimit))
			sys.exit()
		return (startingBibleId, endingBibleId, int(srcTimingErrEst), int(durationErrLimit))


	## return sources if they qualify with quality limit, return all targets that have no source
	def getFilesetSourceAndTargets(self, bibleId, timingEstErr):
		sql = ("SELECT bf.id, bf.hash_id, bft.description AS timing_est_err"
			" FROM bible_filesets bf" 
			" INNER JOIN bible_fileset_connections bfc ON bf.hash_id = bfc.hash_id"
			" LEFT OUTER JOIN bible_fileset_tags bft ON bf.hash_id = bft.hash_id"
			" AND bft.name = 'timing_est_err'"
			" WHERE bfc.bible_id = %s"
			" AND bf.set_type_code IN ('audio', 'audio_drama')"
			" ORDER BY bft.description desc")
		resultSet = self.db.select(sql, (bibleId,))
		sources = []
		targets = []
		for row in resultSet:
			if row[2] == None:
				targets.append((row[0], row[1]))
			elif int(row[2]) <= timingEstErr:
				sources.append((row[0], row[1]))
			else:
				print("%s was not copied, because timing_est_err = %s." % (row[0], row[2]))
		return (sources, targets)


	def compareTwoFilesets(self, srcFilesetId, srcHashId, destFilesetId, destHashId, durationErrLimit):
		sql = ("SELECT bf1.id, bf2.id,"
			" (cast(bf2.duration as signed) - cast(bf1.duration as signed)) AS timing_err_est,"
			" bf1.book_id, bf1.chapter_start"
			" FROM bible_files bf1, bible_files bf2"
			" WHERE bf1.hash_id = %s"
			" AND bf2.hash_id = %s"
			" AND bf1.book_id = bf2.book_id"
			" AND bf1.chapter_start = bf2.chapter_start"
			" ORDER by bf1.file_name;")
		resultSet = self.db.select(sql, (srcHashId, destHashId))
		if len(resultSet) == 0:
			print("%s -> %s was not copied, because there are no books in common." % (srcFilesetId, destFilesetId))
			return None
		durationDeltas = []
		for row in resultSet:
			durationDeltas.append(abs(row[2]))
		median = statistics.median(durationDeltas)
		if median > durationErrLimit:
			print("%s -> %s was not copied, because the median duration delta was %d." % (srcFilesetId, destFilesetId, median))
			return None
		return resultSet


	def copyTimings(self, hashId, resultSet):
		statements = []
		deleteStmt = ("DELETE FROM bible_file_timestamps WHERE bible_file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id = %s)")
		values = [(hashId,)]
		statements.append((deleteStmt, values))
		deleteTagStmt = ("DELETE FROM bible_file_tags WHERE file_id IN"
			" (SELECT id FROM bible_files WHERE hash_id = %s)"
			" AND tag IN ('timing_id', 'timing_est_err')")	
		statements.append((deleteTagStmt, values))
		insertStmt = ("INSERT INTO bible_file_timestamps (bible_file_id, verse_start, verse_end, `timestamp`)"
			" SELECT %s, verse_start, verse_end, `timestamp`"
			" FROM bible_file_timestamps WHERE bible_file_id = %s")
		values = []
		for row in resultSet:
			values.append((row[1], row[0]))
		statements.append((insertStmt, values))
		timingIdStmt = ("INSERT INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%s, 'timing_id', %s, 0)")
		statements.append((timingIdStmt, values))
		timingErrStmt = ("INSERT INTO bible_file_tags (file_id, tag, value, admin_only) VALUES (%s, 'timing_est_err', %s, 0)")	
		values = []
		for row in resultSet:
			values.append((row[1], row[2]))
		statements.append((timingErrStmt, values))
		self.db.executeTransaction(statements)


	def process(self):
		(startBibleId, endBibleId, srcTimingErrEst, durationErrLimit) = self.getCommandLine()
		bibleIdList = self.db.selectList("SELECT id FROM bibles WHERE id BETWEEN %s AND %s", (startBibleId, endBibleId))
		for bibleId in bibleIdList:
			(sourceList, targetList) = self.getFilesetSourceAndTargets(bibleId, srcTimingErrEst)
			for (targetFilesetId, targetHashId) in targetList:
				if len(sourceList) == 0:
					print("? -> %s was not copied, because there is no source." % (targetFilesetId))
				else:
					for (sourceFilesetId, sourceHashId) in sourceList:
						fileIds = self.compareTwoFilesets(sourceFilesetId, sourceHashId, targetFilesetId, targetHashId, durationErrLimit)
						if fileIds != None:
							print("%s -> %s %d Timestamp rows inserted." % (sourceFilesetId, targetFilesetId, len(fileIds)))
							self.copyTimings(targetHashId, fileIds)
		self.db.close()


copy = BibleFileTimestamps_Copy()
copy.process()

## getFilesetSourceAndTarget
"""
SELECT bf.id, bf.hash_id, bft.description AS timing_est_err
FROM bible_filesets bf 
INNER JOIN bible_fileset_connections bfc ON bf.hash_id = bfc.hash_id
LEFT OUTER JOIN bible_fileset_tags bft ON bf.hash_id = bft.hash_id AND bft.name = 'timing_est_err'
WHERE bfc.bible_id = 'ENGESV'
AND bf.set_type_code IN ('audio', 'audio_drama')
ORDER BY bft.description desc;
"""

## compareDurations of two filesets
"""
SELECT bf1.id, bf2.id, bf1.book_id, bf1.chapter_start,
abs(cast(bf1.duration as signed) - cast(bf2.duration as signed)) as timing_err_est
FROM bible_files bf1, bible_files bf2
WHERE bf2.hash_id = '2348543d7b4b' -- 'ENGESVN2DA'
AND bf1.hash_id = '4b3ad0194aee' -- 'ENGESVC2DA16'
AND bf1.book_id = bf2.book_id
AND bf1.chapter_start = bf2.chapter_start
ORDER by bf1.file_name;
"""
