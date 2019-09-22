# SQLUtility.py
#
# This program provides convenience methods wrapping the MySQL client.
#

import sys
import pymysql
from Config import *

class SQLUtility:

	def __init__(self, host, port, user, db_name):
		self.conn = pymysql.connect(host = host,
                             		user = user,
                             		password = os.environ['MYSQL_PASSWD'],
                             		db = db_name,
                             		port = port,
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
		except Exception as err:
			self.error(cursor, statement, err)


	def executeBatch(self, statement, valuesList):
		cursor = self.conn.cursor()
		try:
			cursor.execute("BEGIN", None)
			for value in valuesList:
				#print("values", len(value), value)
				cursor.execute(statement, value)
			self.conn.commit()
			cursor.close()
		except Exception as err:
			self.error(cursor, statement, err)


	def select(self, statement, values):
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			resultSet = cursor.fetchall()
			cursor.close()
			return resultSet
		except Exception as err:
			self.error(cursor, statement, err)


	def selectScalar(self, statement, values):
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			result = cursor.fetchone()
			cursor.close()
			return result[0] if result != None else None
		except Exception as err:
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
		sys.exit()


"""
## Unit Test
config = Config()
sql = SQLUtility(config)
count = sql.selectScalar("select count(*) from language_status", None)
print(count)
lista = sql.selectList("select title from language_status", None)
print(lista)
mapa = sql.selectMap("select id, title from language_status", None)
print(mapa)
mapb = sql.selectMapList("select id, title from language_status", None)
print(mapb)
sql.close()
"""