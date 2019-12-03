# BibleFileTimings_Copy.py

# This program copies bible_file_timestamp data between sister filesets (eg N1 and N2 for a given bible), 
# if the corresponding chapter durations are within some epsilon (eg a second by default, 
# but setable via command line argument).
#
# Usage: python3 py/BibleFileTimings_Copy.py 


import sys
import os
import io
from SQLUtility import *


DUR_HOST = "localhost"
DUR_USER = "root"
DUR_PORT = 3306
DUR_DB_NAME = "dbp"


class BibleFileTimings_Copy:

	def __init__(self):
		self.db = SQLUtility(DUR_HOST, DUR_PORT, DUR_USER, DUR_DB_NAME)


	def getCommandLine(self):
		if len(sys.argv) < 3:
			print("Usage: BibleFileTimings_Copy.py starting_bible_id ending_bible_id")
			sys.exit()
		return(sys.argv[1], sys.argv[2])


	def selectDurations(self, filesetId):
		sql = ("SELECT concat(book_id, ':', chapter_start), duration FROM bible_files WHERE hash_id IN"
				" (SELECT hash_id FROM bible_filesets WHERE id = %s)")
		return self.db.selectMap(sql, (filesetId,))
		

	def process(self):
		(startingId, endingId) = self.getCommandLine()
		bibleIdList = self.db.selectList("SELECT id FROM bibles WHERE id BETWEEN %s AND %s", (startingId, endingId))
		print(bibleIdList)
		for bibleId in bibleIdList:
			sql = ("SELECT id FROM bible_filesets bf, bible_fileset_connections bfc"
					" WHERE bf.hash_id = bfc.hash_id AND bfc.bible_id = %s"
					" AND bf.set_type_code IN ('audio', 'audio_drama')")
			filesetIdList = self.db.selectList(sql, (bibleId,))	
			print(filesetIdList)		
			#startingFilesetId = filesetIdList[0]
			startingFilesetId = "ENGESVC2DA16"
			startingDurationMap = self.selectDurations(startingFilesetId) # book:chapter -> duration
			for filesetId in filesetIdList:
				if filesetId != startingFilesetId:
					durationMap = self.selectDurations(filesetId)
					for bookChapter, duration in startingDurationMap.items():
						compareDuration = durationMap.get(bookChapter)
						if compareDuration != None:# and abs(duration - compareDuration) > 1:
							print("%s,%s,%s,%s,%d" % (bibleId, startingFilesetId, filesetId, bookChapter, (duration - compareDuration)))
							#print(bibleId, startingFilesetId, filesetId, bookChapter, (duration - compareDuration))
		self.db.close()


duration = BibleFileTimings_Copy()
duration.process()




