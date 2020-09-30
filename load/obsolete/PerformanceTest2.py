# PerformanceTest2.py
#
# This test selects a set of rows into a list
# Then it repeatedly inserts them into temporary tables
# Each test inserts the same number of records, 
# but changes the 

import time
import random
from Config import *
from SQLUtility import *


class PerformanceTest2:

	def __init__(self, config, db):
		self.config = config
		self.db = db
		self.selectedList = None

	def process(self):
		self.selectedList = self.selectRows()
		numRecs = len(self.selectedList)
		for numBatches in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384]:
			batchSize = int(numRecs / numBatches)
			self.insertTest(numBatches, batchSize)

	def selectRows(self):
		startTime = time.time()
		result =  db.select("SELECT hash_id, file_name FROM bible_files limit 16384", ())		
		testName = "Select %d rows from bible_files buffered fetchAll" % (len(result))
		self.duration(startTime, testName)
		return result

	def insertTest(self, numBatches, batchSize):
		startTime = time.time()
		tableName = "test%d" % (numBatches)
		sql = "CREATE TEMPORARY TABLE %s (hash_id varchar(20), file_name varchar(128))" % (tableName)
		db.execute(sql, ())
		values = self.selectedList[0:batchSize]
		sql = "INSERT INTO " + tableName + " (hash_id, file_name) VALUES (%s, %s)"
		for testRun in range(0, numBatches):
			db.executeBatch(sql, values)
		testName = "Insert %d batches of %d rows each" % (numBatches, batchSize)		
		self.duration(startTime, testName)
		sql = "SELECT count(*) FROM %s" % (tableName)
		count = db.selectScalar(sql, ())
		#print("%d records in %s" % (count, tableName))

	def duration(self, start, testName):
		duration = time.time() - start
		print(testName, "{:.3f}".format(duration), "sec")


if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	test = PerformanceTest2(config, db)
	test.process()
	db.close()

