# SQLBatchExec

# This program prepares and executes a single batch of SQL.

import io
import os
import sys


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
		# open file with timestamp
		# write each statement into file
		# close file
		# login into the correct mysql server and database
		# exec file to mysql
		print("tbd")




