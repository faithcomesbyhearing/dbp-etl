# SQLBatchExec

# This program prepares and executes a single batch of SQL.

import io
import os
import sys
import re
import time
from datetime import datetime
import subprocess
from Config import *
from DBPRunFilesS3 import *


class SQLBatchExec:

	def __init__(self, config):
		self.config = config
		self.statements = []
		self.counts = []
		self.unquotedRegex = re.compile(r"(^.*)(\'@.*?m3u8\')(.*$)")


	def insert(self, tableName, pkeyNames, attrNames, values, keyPosition=None):
		if len(values) > 0:
			names = attrNames + pkeyNames
			valsubs = ["'%s'"] * len(names)
			sql = "INSERT INTO %s (%s) VALUES (%s);" % (tableName, ", ".join(names), ", ".join(valsubs))
			for value in values:
				stmt = sql % value
				stmt = self.unquoteValues(stmt)
				self.statements.append(stmt)
				if keyPosition != None:
					keyValue = value[keyPosition].replace("-", "_")
					self.statements.append("SET @%s = LAST_INSERT_ID();" % (keyValue,))
			self.counts.append(("insert", tableName, len(values)))


	def insertSet(self, tableName, pkeyNames, attrNames, values):
		if len(values) > 0:
			names = attrNames + pkeyNames
			stmt = "INSERT INTO %s (%s) VALUES " % (tableName, ", ".join(names))
			self.statements.append(stmt)
			valsubs = ["'%s'"] * len(names)
			sql = "(%s)," % (", ".join(valsubs))
			lastSql = "(%s);" % (", ".join(valsubs))
			lastValueIndex = len(values) - 1
			for i in range(len(values)):
				value = values[i]
				if i < lastValueIndex:
					stmt = sql % value
				else:
					stmt = lastSql % value
				stmt = self.unquoteValues(stmt)
				self.statements.append(stmt)
			self.counts.append(("insert", tableName, len(values)))		


	def unquoteValues(self, stmt):
		match = self.unquotedRegex.match(stmt)
		if match != None:
			stmt = match.group(1) + match.group(2).replace("'", "") + match.group(3)
		stmt = stmt.replace("'None'", "NULL")
		return stmt


	def update(self, tableName, pkeyNames, attrNames, values):
		if len(values) > 0:
			sql = "UPDATE %s SET %s WHERE %s;" % (tableName, "='%s', ".join(attrNames) + "='%s'", "='%s' AND ".join(pkeyNames) + "='%s'")
			for value in values:
				stmt = sql % value
				stmt = stmt.replace("'None'", "NULL")
				self.statements.append(stmt)
			self.counts.append(("update", tableName, len(values)))


	def updateCol(self, tableName, pkeyNames, values):
		if len(values) > 0:
			for value in values:
				colName = value[0]
				colValue = "'" + str(value[1]) + "'"
				oldValue = value[2]
				pkeyValues = value[3:]
				sql = "UPDATE %s SET %s = %s WHERE %s; -- prior=%s" % (tableName, colName, colValue, "='%s' AND ".join(pkeyNames) + "='%s'", oldValue)
				stmt = sql % pkeyValues
				stmt = stmt.replace("'None'", "NULL")
				self.statements.append(stmt)
			self.counts.append(("updateCol", tableName, len(values)))


	def delete(self, tableName, pkeyNames, pkeyValues):
		if len(pkeyValues) > 0:
			sql = "DELETE FROM %s WHERE %s;" % (tableName, "='%s' AND ".join(pkeyNames) + "='%s'")
			for value in pkeyValues:
				stmt = sql % value
				stmt = stmt.replace("'None'", "NULL")
				self.statements.append(stmt)
			self.counts.append(("delete", tableName, len(pkeyValues)))


	def rawStatement(self, stmt):
		self.statements.append(stmt)


	def displayCounts(self):
		finalCount = {}
		finalSequence = []
		## Summarize counts
		for (tran, tableName, count) in self.counts:
			key = (tran, tableName)
			if key not in finalSequence:
				finalSequence.append(key)
			counts = finalCount.get(key, 0)
			counts += count
			finalCount[key] = counts
		## Display counts
		for (tran, tableName) in finalSequence:
			counts = finalCount[(tran, tableName)]
			print(tran, tableName, counts)


	def displayStatements(self):
		for statement in self.statements:
			print(statement)


	def execute(self, batchName):
		if len(self.statements) == 0:
			print("NO INSERT, UPDATE, or DELETE Transactions to process")
			return True
		else:
			pattern = self.config.filename_datetime 
			tranDir = "./" ## we need a config parameter
			path = tranDir + "Trans-" + batchName + ".sql"
			print("Transactions", path)
			tranFile = open(path, "w")
			tranFile.write("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n")
			tranFile.write("START TRANSACTION;\n")
			for statement in self.statements:
				tranFile.write(statement + "\n")
			tranFile.write("COMMIT;\n")
			tranFile.write("EXIT\n")
			tranFile.close()
			DBPRunFilesS3.uploadFile(self.config, path)
			startTime = time.perf_counter()
			if self.config.database_tunnel != None:
				results1 = os.popen(self.config.database_tunnel).read()
				print("tunnel opened:", results1)
			with open(path, "r") as sql:
				cmd = [self.config.mysql_exe, "-h", self.config.database_host, 
						"-P", str(self.config.database_port),
						"-u", self.config.database_user,
						"-p" + self.config.database_passwd,
						self.config.database_db_name]
				response = subprocess.run(cmd, shell=False, stdin=sql, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=2400)
				success = response.returncode == 0
				print("SQLBATCH:", str(response.stderr.decode('utf-8')))
				duration = (time.perf_counter() - startTime)
				print("SQLBATCH execution time", round(duration, 2), "sec for", batchName)
			self.statements = []
			self.counts = []
			return success
				

if (__name__ == '__main__'):
	config = Config()
	sql = SQLBatchExec(config)
	sql.statements.append("SELECT * FROM bibles;")
	sql.statements.append("SHOW DATABASES;")
	values = []
	values.append(("a", "b", "c"))
	values.append(("d", "e", "f"))
	values.append(("gg", "hh", "ii"))
	values.append(("jjj", "kkk", "lll"))
	sql.insertSet("test_table", ("pkey",), ("attr1", "attr2"), values)
	sql.displayStatements()
	sql.counts.append(("insert", "tablea", 12))
	sql.counts.append(("insert", "tablea", 24))
	sql.counts.append(("insert", "table0", 36))
	sql.counts.append(("delete", "table9", 1))
	sql.displayCounts()
	#sql.execute("test")



