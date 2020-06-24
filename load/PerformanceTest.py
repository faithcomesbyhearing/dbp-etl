# PerformanceTest.py
#
#0. Some arbitrary calculation repeated many times.
#1. Selecting a large number of records without processing them.
#2. Selecting the same records, but building a large in-memory array.
#3. Create a temporary table
#4. Insert records into the temporary table from a select like step 1
#5. Insert records into the temporary table from a large in-memory array.
#6. Repeating steps 4 and 5 with for both batch and single record commit
#And possibly some other things will occur to me overnight.

import time
import random
from Config import *
from SQLUtility import *


class PerformanceTest:

	def __init__(self, config, db):
		self.config = config
		self.db = db


	def process(self):
		self.cpuIntensive()
		self.selectOnly1()
		self.selectOnly2()
		self.selectOnly3()
		self.selectAndMap()
		self.selectAndInsertInto()
		self.selectAndInsertBatch()
		self.selectAndInsertRows()


	def cpuIntensive(self):
		startTime = time.time()
		for index in range(0, 10000000): # random 10 million
			rand = random.random()
		self.duration(startTime, "Test 1. CPU Test, random 10 million times")

	def selectOnly1(self):
		startTime = time.time()
		#resultSet = db.select("SELECT hash_id, id, asset_id, set_type_code, set_size_code FROM bible_filesets", ())
		resultSet = db.select("SELECT hash_id, file_name FROM bible_files limit 500000", ())
		testName = "Test 2. Select %d rows from bible_files buffered fetchAll" % (len(resultSet))
		self.duration(startTime, testName)

	def selectOnly2(self):
		startTime = time.time()
		cursor = db.conn.cursor()
		try:
			count = 0
			cursor.execute("SELECT hash_id, file_name FROM bible_files limit 500000", ())
			row = cursor.fetchone()
			while(row != None):
				row = cursor.fetchone()
				count += 1
			cursor.close()
		except Exception as err:
			print("ERROR in selectOnly2", err)
		self.duration(startTime, "Test 3. Select %d rows from bible_files buffered fetchone" % (count))

	def selectOnly3(self):
		startTime = time.time()
		unbufferedConn = pymysql.connect(host = self.config.database_host,
                             		user = self.config.database_user,
                             		password = self.config.database_passwd,
                             		db = self.config.database_db_name,
                             		port = self.config.database_port,
                             		charset = 'utf8mb4',
                             		cursorclass = pymysql.cursors.SSCursor)
		cursor = unbufferedConn.cursor()
		try:
			count = 0
			cursor.execute("SELECT hash_id, file_name FROM bible_files limit 500000", ())
			row = cursor.fetchone()
			while(row != None):
				row = cursor.fetchone()
				count += 1
			cursor.close()
			unbufferedConn.close()
		except Exception as err:
			print("ERROR in selectOnly3", err)
		self.duration(startTime, "Test 4. Select %d rows from bible_files unbuffered fetchone" % (count))

	def selectAndMap(self):
		startTime = time.time()
		hashMap = {}
		resultSet = db.select("SELECT hash_id, file_name FROM bible_files limit 500000", ())
		#resultSet = db.select("SELECT hash_id, id, asset_id, set_type_code, set_size_code FROM bible_filesets", ())
		for row in resultSet:
			hashMap[row[0]] = row
		testName = "Test 5. Select and hashMap %d rows from bible_files" % (len(resultSet))
		self.duration(startTime, testName)

	def selectAndInsertInto(self):
		startTime = time.time()
		db.execute("CREATE TEMPORARY TABLE test6 SELECT hash_id, file_name FROM bible_files limit 500000",())
		count = db.selectScalar("SELECT count(*) from test6", ())
		testName = "Test6. Insert into Temp Table %d rows" % (count)
		self.duration(startTime, testName)

	def selectAndInsertBatch(self):
		startTime = time.time()
		db.execute("CREATE TEMPORARY TABLE test7 (hash_id varchar(20), file_name varchar(128))", ())
		resultSet = db.select("SELECT hash_id, file_name FROM bible_files limit 300000", ())
		values = []
		for row in resultSet:
			values.append([str(row[0]), str(row[1])])
		db.executeBatch("INSERT INTO test7 (hash_id, file_name) VALUES (%s, %s)", values)
		testName = "Test7. Select and Insert into Temp Table %d rows as batch" % (len(values))
		self.duration(startTime, testName)

	def selectAndInsertRows(self):
		startTime = time.time()
		db.execute("CREATE TEMPORARY TABLE test8 (hash_id varchar(20), file_name varchar(128))", ())
		resultSet = db.select("SELECT hash_id, file_name FROM bible_files limit 30000", ())
		for row in resultSet:
			db.execute("INSERT INTO test8 (hash_id, file_name) VALUES (%s, %s)", row)
		testName = "Test8. Select and Insert into Temp Table %d rows one row in a batch" % (len(resultSet))
		self.duration(startTime, testName)

	def duration(self, start, testName):
		duration = time.time() - start
		print(testName, "{:.3f}".format(duration), "sec")



if (__name__ == '__main__'):
	config = Config()
	db = SQLUtility(config)
	test = PerformanceTest(config, db)
	test.process()
	db.close()