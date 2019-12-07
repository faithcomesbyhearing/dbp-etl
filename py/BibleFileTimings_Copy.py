# BibleFileTimings_Copy.py

# This program copies bible_file_timestamp data between sister filesets (eg N1 and N2 for a given bible), 
# if the corresponding chapter durations are within some delta (eg a second by default, 
# but setable via command line argument).
#
# Usage: python3 py/BibleFileTimings_Copy.py
#
# 1. Read in two parameters, quality to permit copying, allowed average duration error to allow copying
# 2. Group audio filesets by Bible
# 3. Within Bible group look for filesets with reliable timings less or equal to than quality code x (input param)
# 4. Look for other filesets within the same Bible group that do not have timings
# 5. Compare durations for each file within fileset, take difference
# 6. Average differences, and compare to allowed average duration error
# 7. If the difference is within range, copy the timestamps
# 8. During copy store the duration difference in bible_files.timing_est_err
# 9. During copy store the source file_id in bible_files.timing_src of the file whose timing is copied

# Note: there is also a need to recopy a fileset that has already been copied, because the source
# timestamps have improved.

# Note: if the BibleFileTimings_Insert program is given a reliability parameter,
# how does this affect the ability to process over multiple Bibles.
# Would it be better if the reliability were stored somewhere?

# Consider storing timing_est_err in bible_file_tags for duration errors
# Consider storing timing_est_err in bible_fileset_tags for raw copies error estimate
# Consider storing timing_src in bible_file_tags which then does not have foreign key.


import sys
import os
import io
import statistics
from SQLUtility import *


DUR_HOST = "localhost"
DUR_USER = "root"
DUR_PORT = 3306
DUR_DB_NAME = "dbp"


class BibleFileTimings_Copy:

	def __init__(self):
		self.db = SQLUtility(DUR_HOST, DUR_PORT, DUR_USER, DUR_DB_NAME)


	# One option does all that can be done, skipping any already copied
	# Another option does a specific bible_id, and does copy even when data exists
	def getCommandLine(self):
		if len(sys.argv) < 3:
			print("Usage: BibleFileTimings_Copy.py source_err_limit_code duration_err_limit_in_sec [bible_id]")
			sys.exit()
		bibleId = sys.argv[3] if len(sys.argv) == 4 else None
		return(sys.argv[1], sys.argv[2], bibleId)


	## return a source if it qualifies with quality limit, return all targets that have no source
	def getFilesetSourceAndTargets(self, bibleId, qualityLimit):
		sql = ("SELECT bf.id, bft.description AS timing_est_err"
			" FROM bible_filesets bf" 
			" INNER JOIN bible_fileset_connections bfc ON bf.hash_id = bfc.hash_id"
			" LEFT OUTER JOIN bible_fileset_tags bft ON bf.hash_id = bft.hash_id"
			" AND bft.name = 'timing_est_err'"
			" WHERE bfc.bible_id = %s"
			" AND bf.set_type_code IN ('audio', 'audio_drama')"
			" ORDER BY bft.description")
		resultSet = self.db.select(sql, (bibleId,))
		targets = []
		for row in resultSet:
			if row[1] == None:
				targets.append(row[0])
			else:
				if row[1] <= qualityLimit and len(targets) > 0:
					return (row[0], targets)
		return (None, None)


	def selectDurations(self, filesetId):
		sql = ("SELECT concat(book_id, ':', chapter_start), duration FROM bible_files WHERE hash_id IN"
				" (SELECT hash_id FROM bible_filesets WHERE id = %s)")
		return self.db.selectMap(sql, (filesetId,))


	def copyTimings(self, srcFilesetId, destFilesetId):
		sql = ("INSERT INTO bible_file_timestamps (bible_file_id, verse_start, verse_end, `timestamp`"
			" SELECT ")
		# insert select to copy the timings
		

	def process(self):
		(qualityLimit, durationErrLimit, bibleId) = self.getCommandLine()
		if bibleId != None:
			bibleIdList = [bibleId]
		else:
			bibleIdList = self.db.selectList("SELECT id FROM bibles")
			print(bibleIdList)
			for bibleId in bibleIdList:
				(srcFilesetId, targetList = self.getFilesetSourceAndTargets(bibleId, qualityLimit)
				if srcFilesetId != None:
					print(srcFilesetId)
					srcDurationMap = self.selectDurations(srcFilesetId) # book:chapter -> duration

					for destFilesetId in targetList:
						durationMap = self.selectDurations(destFilesetId)
						durationDeltas = []
						for bookChapter, duration in srcDurationMap.items():
							compareDuration = durationMap.get(bookChapter)
							if compareDuration != None:
								durationDeltas.append((duration - compareDuration))
								print("%s,%s,%s,%s,%d" % (bibleId, srcFilesetId, destFilesetId, bookChapter, (duration - compareDuration)))
						avgDuration = statistics.mean(durationDeltas)
						if avgDuration < durationErrLimit:
							self.copyTimings(srcFilesetId, destFilesetId)

		self.db.close()


duration = BibleFileTimings_Copy()
duration.process()

## getFilesetSourceAndTarget
"""
SELECT bf.id, bft.description AS timing_est_err
FROM bible_filesets bf 
INNER JOIN bible_fileset_connections bfc ON bf.hash_id = bfc.hash_id
LEFT OUTER JOIN bible_fileset_tags bft ON bf.hash_id = bft.hash_id AND bft.name = 'timing_est_err'
WHERE bfc.bible_id = 'ENGESV'
AND bf.set_type_code IN ('audio', 'audio_drama')
ORDER BY bft.description;
"""

## Query to get all timings for a fileset
## Do I need to do this one file at a time
"""
SELECT bft.bible_file_id, bft.verse_start, bft.verse_end, bft.`timestamp` 
FROM bible_file_timestamps bft
INNER JOIN bible_files bf ON bft.bible_file_id = bf.id
INNER JOIN bible_filesets bs ON bf.hash_id = bs.hash_id
AND bs.id = 'ENGKJVN2DA16'


"""

"""
SELECT bf.id FROM bible_files bf
JOIN bible_filesets bs ON bf.hash_id = bs.hash_id
AND bs.id = 'ENGKJVN2DA';

"""




