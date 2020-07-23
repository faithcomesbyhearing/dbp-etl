# SQLBatchExec

# This program prepares and executes a single batch of SQL.

import io
import os
import sys
from datetime import datetime
from Config import *


class SQLBatchExec:

	def __init__(self, config):
		self.config = config
		self.statements = []
		self.counts = []


	def insert(self, tableName, attrNames, pkeyNames, values):
		if len(values) > 0:
			names = attrNames + pkeyNames
			valsubs = ["'%s'"] * len(names)
			sql = "INSERT INTO %s (%s) VALUES (%s);" % (tableName, ", ".join(names), ", ".join(valsubs)) 
			for value in values:
				self.statements.append(sql % value)
			self.counts.append(("insert", tableName, len(values)))


	def update(self, tableName, attrNames, pkeyNames, values):
		if len(values) > 0:
			sql = "UPDATE %s SET %s WHERE %s;" % (tableName, "='%s', ".join(attrNames) + "='%s'", "='%s' AND ".join(pkeyNames) + "='%s'")
			for value in values:
				self.statements.append(sql % value)
			self.counts.append(("update", tableName, len(values)))


	def delete(self, tableName, pkeyNames, pkeyValues):
		if len(pkeyValues) > 0:
			sql = "DELETE FROM %s WHERE %s;" % (tableName, "='%s' AND ".join(pkeyNames) + "='%s'")
			for value in pkeyValues:
				self.statements.append(sql % value)
			self.counts.append(("delete", tableName, len(pkeyValues)))


	def displayCounts(self):
		for (tran, tableName, count) in self.counts:
			print(tran, tableName, count)


	def displayStatements(self):
		for statement in self.statements:
			print(statement)


	def execute(self):
		pattern = self.config.filename_datetime 
		tranDir = "./" ## we need a config parameter
		path = tranDir + "Trans-" + datetime.today().strftime(pattern) + ".sql"
		print("Transactions", path)
		tranFile = open(path, "w")
		for statement in self.statements:
			tranFile.write(statement + "\n")
		tranFile.close()
		cmd = "mysql -u%s -p%s  %s < %s" % (self.config.database_user, 
											self.config.database_passwd,
											self.config.database_db_name,
											path)
		print("cmd", cmd)
		results = os.popen(cmd).read()
		print(results)
		# login into the correct mysql server and database
		# exec file to mysql


if (__name__ == '__main__'):
	config = Config()
	sql = SQLBatchExec(config)
	sql.statements.append("SELECT count(*) FROM bibles")
	sql.displayStatements()
	sql.displayCounts()
	sql.execute()





