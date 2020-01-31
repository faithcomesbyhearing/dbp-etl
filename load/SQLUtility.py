# SQLUtility.py
#
# This program provides convenience methods wrapping the MySQL client.
#

import os
import sys
import pymysql

class SQLUtility:

	def __init__(self, config, cursor = 'list'):
		if cursor == 'dict':
			pycursor = pymysql.cursors.DictCursor
		else:
			pycursor = pymysql.cursors.Cursor
					
		self.conn = pymysql.connect(host = config.database_host,
                             		user = config.database_user,
                             		password = config.database_passwd,
                             		db = config.database_db_name,
                             		port = config.database_port,
                             		charset = 'utf8mb4',
                             		cursorclass = pycursor)

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
			cursor.executemany(statement, valuesList)
			self.conn.commit()
			cursor.close()
		except Exception as err:
			self.error(cursor, statement, err)


	def executeTransaction(self, statements):
		cursor = self.conn.cursor()
		try:
			for statement in statements:
				cursor.executemany(statement[0], statement[1])
			self.conn.commit()
			cursor.close()
		except Exception as err:
			self.error(cursor, statement, err)


	def select(self, statement, values):
		#print("SQL:", statement, values)
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			resultSet = cursor.fetchall()
			cursor.close()
			return resultSet
		except Exception as err:
			self.error(cursor, statement, err)


	def selectScalar(self, statement, values):
		#print("SQL:", statement, values)
		cursor = self.conn.cursor()
		try:
			cursor.execute(statement, values)
			result = cursor.fetchone()
			cursor.close()
			return result[0] if result != None else None
		except Exception as err:
			self.error(cursor, statement, err)


	def selectRow(self, statement, values):
		resultSet = self.select(statement, values)
		return resultSet[0] if len(resultSet) > 0 else None


	def selectSet(self, statement, values):
		resultSet = self.select(statement, values)
		results = set()
		for row in resultSet:
			results.add(row[0])
		return results		


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