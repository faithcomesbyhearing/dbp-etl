# SQLBatchExec

# This program prepares and executes a single batch of SQL.

import io
import os
import sys
import re
from datetime import datetime
import subprocess
from Config import *


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


	def rawStatment(self, stmt):
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
			path = tranDir + "Trans-" + datetime.today().strftime(pattern) + "-" + batchName + ".sql"
			print("Transactions", path)
			tranFile = open(path, "w")
			tranFile.write("START TRANSACTION;\n")
			for statement in self.statements:
				tranFile.write(statement + "\n")
			tranFile.write("COMMIT;\n")
			tranFile.close()
			if self.config.database_tunnel != None:
				results1 = os.popen(self.config.database_tunnel).read()
				print("tunnel opened:", results1)
			cmd = ("mysql -h %s -P %s -u %s -p%s  %s < %s" % 
												(self.config.database_host,
												self.config.database_port,
												self.config.database_user, 
												self.config.database_passwd,
												self.config.database_db_name,
												path))
			response = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=600)
			success = response.returncode == 0
			print("SQLBATCH:", response.stderr)
			return success


if (__name__ == '__main__'):
	config = Config()
	sql = SQLBatchExec(config)
	sql.statements.append("SELECT * FROM bibles;")
	sql.statements.append("SHOW DATABASES;")
	sql.displayStatements()
	sql.counts.append(("insert", "tablea", 12))
	sql.counts.append(("insert", "tablea", 24))
	sql.counts.append(("insert", "table0", 36))
	sql.counts.append(("delete", "table9", 1))
	sql.displayCounts()
	sql.execute("test")



