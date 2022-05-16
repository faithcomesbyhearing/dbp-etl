# SQLUtility.py
#
# This program provides convenience methods wrapping the MySQL client.
#

import os
import sys
import pymysql
from Config import *


class SQLUtility:

	def __init__(self, config, cursor = 'list'):
		if cursor == 'dict':
			pycursor = pymysql.cursors.DictCursor
		else:
			pycursor = pymysql.cursors.Cursor

		if config.database_tunnel != None:
			results1 = os.popen(config.database_tunnel).read()
			print("tunnel opened:", results1)

		database = config.getCurrentDatabaseDBName() if config.getCurrentDatabaseDBName() != None else config.database_db_name
		self.conn = pymysql.connect(
			host = config.database_host,
			user = config.database_user,
			password = config.database_passwd,
			db = database,
			port = config.database_port,
			charset = 'utf8mb4',
			cursorclass = pycursor)
		print("Database '%s' is opened." % (database))

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


	def executeInsert(self, statement, values):
		cursor = self.conn.cursor()
		try :
			cursor.execute(statement, values)
			self.conn.commit()
			lastRow = cursor.lastrowid
			cursor.close()
			return lastRow
		except Exception as err:
			self.error(cursor, statement, err)
			return None


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


	def displayTransaction(self, statements):
		for statement in statements:
			for values in statement[1][:100]: # do first 100 only
				print(statement[0] % values)


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


	def selectMapSet(self, statement, values):
		resultSet = self.select(statement, values)
		results = {}
		for row in resultSet:
			values = results.get(row[0], set())
			values.add(row[1])
			results[row[0]] = values
		return results		


	def error(self, cursor, stmt, error):
		cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (error, stmt))
		self.conn.rollback()
		sys.exit()


if (__name__ == '__main__'):
	config = Config()

	## dbp
	config.setCurrentDatabaseDBName('dbp')
	sql = SQLUtility(config)
	resultSet = sql.select("SELECT count(*) FROM bibles", ())
	print("SELECT count(*) FROM bibles", ())
	for row in resultSet:
		print(row)
	sql.close()

	## dbp_users
	config.setCurrentDatabaseDBName('dbp_users')
	sql = SQLUtility(config)
	resultSet = sql.select("SELECT count(*) FROM users", ())
	print("SELECT count(*) FROM users", ())
	for row in resultSet:
		print(row)
	sql.close()

	## LANGUAGE
	config.setCurrentDatabaseDBName('LANGUAGE')
	sql = SQLUtility(config)
	resultSet = sql.select("SELECT count(*) FROM bible", ())
	print("SELECT count(*) FROM bible")
	for row in resultSet:
		print(row)
	sql.close()

	## BIBLEBRAIN
	config.setCurrentDatabaseDBName('BIBLEBRAIN')
	sql = SQLUtility(config)
	resultSet = sql.select("SELECT count(*) FROM fileset", ())
	print("SELECT count(*) FROM fileset")
	for row in resultSet:
		print(row)
	sql.close()
