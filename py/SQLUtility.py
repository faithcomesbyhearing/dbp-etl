# SQLUtility.py
#
# This program provides convenience methods wrapping the MySQL client.
#

import pymysql
from Config import *

class SQLUtility:

	def __init__(self, config):
		self.conn = pymysql.connect(host = config.database_host,
                             		user = config.database_user,
                             		password = os.environ['MYSQL_PASSWD'],
                             		db = config.database_db_name,
                             		port = config.database_port,
                             		charset = 'utf8mb4',
                             		cursorclass = pymysql.cursors.Cursor)


	def close(self):
		if self.conn != None:
			self.conn.close()
			self.conn = None


	def execute(self, statement, values):
		cursor = self.conn.cursor()
		try :
			cursor.execute(statement, values)
			self.conn.commit()
			cursor.close()
		except Exception, err:
			self.error(cursor, statement, err)


	#def executeBatch(self, [statements]):
	# There needs to be some kind of batch object, which contains a list of statements,
	# and each statement has a list of values


	def select(self, statement, values):
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			resultSet = cursor.fetchall()
			cursor.close()
			return resultSet
		except Exception, err:
			self.error(cursor, statment, err)


	def selectScalar(self, statement, values):
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			result = cursor.fetchone()
			cursor.close()
			return result[0]
		except Exception, err:
			self.error(cursor, statement, err)			


	def selectList(self, statement, values):
		resultSet = self.select(statement, values)
		results = []
		for row in resultSet:
			results.append(row[0])
		return results


	def selectMap(self, statement, values):
		resultSet = self.select(statement, values)
		results = {}
		for row in resultSet:
			results[row[0]] = row[1]
		return results


	def selectMapList(self, statement, values):
		resultSet = self.select(statement, values)
		results = {}
		for row in resultSet:
			values = results.get(row[0], [])
			#if value == None:
			#	results[row[0]] = []
			values.append(row[1])
			results[row[0]] = values
		return results


	def error(self, cursor, stmt, error):
		cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (error, stmt))
		self.conn.rollback()


"""
## Unit Test
config = Config()
sql = SQLUtility(config)
count = sql.selectScalar("select count(*) from language_status", None)
print count
lista = sql.selectList("select title from language_status", None)
print lista
mapa = sql.selectMap("select id, title from language_status", None)
print mapa
mapb = sql.selectMapList("select id, title from language_status", None)
print mapb
sql.close()
"""