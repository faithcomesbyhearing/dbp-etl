# SQLUtility.py
#
# This program provides convenience methods wrapping the MySQL client.
#

import os
import sys
import pymysql

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
				cursor.execute("SELECT ROW_COUNT()")
				rowCount = cursor.fetchone()[0]
				#rowCount = cursor.result.message
				print("rowCount", rowCount)
				#rowCount = cursor.SELECT ROW_COUNT();
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
		return resultSet[0] if len(resultSet) > 0 else [None] * 10


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


	def selectMapRecord(self, statement, values):
		resultSet = self.select(statement, values)
		results = {}
		for row in resultSet:
			results[row[0]] = row[1:]	
		return results	


	def error(self, cursor, stmt, error):
		cursor.close()	
		print("ERROR executing SQL %s on '%s'" % (error, stmt))
		self.conn.rollback()
		sys.exit()


	def insertUpdateV1(self, tableName, whereClause, insertUpdateList):
		self._internalInsertUpdateDeleteV1(tableName, whereClause, insertUpdateList, False)

	def insertUpdateDeleteV1(self, tableName, whereClause, insertUpdateList):
		self._internalInsertUpdateDeleteV1(tableName, whereClause, insertUpdateList, True)

	def _internalInsertUpdateDeleteV1(self, tableName, whereClause, insertUpdateList, doDelete):
		self.schema = { 
			"bible_file_timestamps": (("bible_file_id", "verse_start"), ("`timestamp`",), "id"),
			"bible_fileset_tags": (("hash_id", "name", "language_id"), ("description", "admin_only", "notes", "iso"), None),
			"bible_file_tags": (("file_id", "tag"), ("value", "admin_only"), None)
		}
		(pkeys, values, idkey) = self.schema[tableName]
		numKeys = len(pkeys)
		numValues = len(values)

		key = "concat(" + ",':',".join(pkeys) + ")"
		selectList = values
		if idkey != None:
			selectList += (idkey,)
		select = "SELECT %s,%s FROM %s WHERE %s" % (key, ",".join(selectList), tableName, whereClause)
		print(select)
		tableMap = self.selectMapRecord(select, ())
		print("tableMap", len(tableMap.items()))

		insertList = []
		updateList = []
		deleteList = []
		for record in insertUpdateList:
			# Hack
			record2 = []
			for rec in record:
				record2.append(str(rec))

			key = ":".join(record2[:numKeys])
			currentValues = tableMap.get(key)
			if currentValues == None:
				insertList.append(record)
			else:
				whereFields = record[(numKeys + numValues):] if idkey == None else currentValues[numValues:]
				#print("currentValues", currentValues)
				#print("whereFields", whereFields)
				for index in range(numValues):
					curr = currentValues[index]
					newVal = record[index + numKeys]
					if curr != newVal:
						updateList.append(record[numKeys:] + whereFields)
						break
				del tableMap[key]

		for key in tableMap.keys():
			deleteList.append((key.split(":")))

		statements = []
		insert = "INSERT INTO %s (%s, %s) VALUES " % (tableName, ", ".join(pkeys), ", ".join(values))
		insert += "(" + ", ".join(["%s"] * (len(pkeys) + len(values))) + ")"
		#print(insertList)
		#print(insert)
		statements.append((insert, insertList))

		wherePhrases = []
		if idkey != None:
			print("idkey", idkey)
			wherePhrases.append(idkey + "=%s")
		else:
			for col in pkeys:
				wherePhrases.append(col + "=%s")
		setPhrases = []
		for col in values:
			setPhrases.append(col + "=%s" )
		update = "UPDATE %s SET %s WHERE %s" % (tableName, ", ".join(setPhrases), " AND ".join(wherePhrases))
		#print(updateList)
		#print(update)
		statements.append((update, updateList))

		if doDelete:
			delete = "DELETE FROM %s WHERE %s" % (tableName, " AND ".join(wherePhrases))
			statements.append((delete, deleteList))
			#print(deleteList)
			#print(delete)
		self.executeTransaction(statements)





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