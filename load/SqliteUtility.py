# SqliteUtility.py
#
# This program provides convenience methods wrapping the Sqlite3 client.
# This class supports the same interface as SQLUtility whenever possible
#

import os
import sys
import sqlite3
from Config import *

class SqliteUtility:

	def __init__(self, databasePath):
		print("SqliteUtility.. databasePath: %s" % (databasePath))
		self.conn = sqlite3.connect(databasePath)
		self.setForeignKeyConstraint(True)

	def close(self):
		if self.conn != None:
			self.conn.close()
			self.conn = None


	def setForeignKeyConstraint(self, onOff):
		if onOff:
			self.execute("PRAGMA foreign_keys = ON", ())
		else:
			self.execute("PRAGMA foreign_keys = OFF", ())


	def execute(self, statement, values):
		cursor = self.conn.cursor()
		try :
			cursor.execute(statement, values)
			self.conn.commit()
			cursor.close()
		except Exception as err:
			self.error(cursor, statement, err)


#	def executeInsert(self, statement, values):
#		cursor = self.conn.cursor()
#		try :
#			cursor.execute(statement, values)
#			self.conn.commit()
#			lastRow = cursor.lastrowid
#			cursor.close()
#			return lastRow
#		except Exception as err:
#			self.error(cursor, statement, err)
#			return None


	def executeBatch(self, statement, valuesList):
		cursor = self.conn.cursor()
		try:
			cursor.executemany(statement, valuesList)
			self.conn.commit()
			cursor.close()
		except Exception as err:
			self.error(cursor, statement, err)


#	def executeTransaction(self, statements):
#		cursor = self.conn.cursor()
#		try:
#			for statement in statements:
#				cursor.executemany(statement[0], statement[1])
#			self.conn.commit()
#			cursor.close()
#		except Exception as err:
#			self.error(cursor, statement, err)


#	def displayTransaction(self, statements):
#		for statement in statements:
#			for values in statement[1][:100]: # do first 100 only
#				print(statement[0] % values)


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


#	def selectScalar(self, statement, values):
#		#print("SQL:", statement, values)
#		cursor = self.conn.cursor()
#		try:
#			cursor.execute(statement, values)
#			result = cursor.fetchone()
#			cursor.close()
#			return result[0] if result != None else None
#		except Exception as err:
#			self.error(cursor, statement, err)


#	def selectRow(self, statement, values):
#		resultSet = self.select(statement, values)
#		return resultSet[0] if len(resultSet) > 0 else None


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


#	def selectMapList(self, statement, values):
#		resultSet = self.select(statement, values)
#		results = {}
#		for row in resultSet:
#			values = results.get(row[0], [])
#			values.append(row[1])
#			results[row[0]] = values
#		return results


	def error(self, cursor, stmt, error):
		cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (error, stmt))
		self.conn.rollback()
		sys.exit()


if __name__ == "__main__":
	databasePath = ""

	if len(sys.argv) > 2:
		databasePath = sys.argv[2]

	sql = SqliteUtility(databasePath)
	resultSet = sql.select("SELECT rowId, code, heading, title, name, chapters FROM tableContents ORDER BY rowId", ())

	for (rowId, bookId, heading, title, name, chapters) in resultSet:
		print("resultSet rowId: %s, bookId: %s, heading: %s, title: %s, name: %s, chapters: %s" % (rowId, bookId, heading, title, name, chapters))

	# resultSet = sql.select("SELECT reference, text FROM verses", ())
	# resultSet = sql.select("SELECT reference, text FROM verses WHERE reference LIKE 'PSA:5:%' OR reference LIKE 'PSA:4:%'", ())
	# resultSet = sql.select("SELECT reference, text FROM verses WHERE reference LIKE 'GEN:2:%' ORDER BY rowId", ())
	resultSet = sql.select("SELECT reference FROM verses WHERE reference LIKE 'GEN:2:%' ORDER BY rowId", ())

	# for (reference, verseText) in resultSet:
	for (reference) in resultSet:
		# print("resultSet reference: %s, verseText: %s" % (reference, verseText))
		print("reference: %s," % (reference))

	sql.close()
