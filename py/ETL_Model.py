# ETL_Model.py
#
# This program classes that are used to internally represent the ETL Model XML file.
#

import sys

def checkElementName(expectNames, node):
	if node.nodeName not in expectNames:
		print("Node name must be %s, not %s" % (expectNames, node.nodeName))
		sys.exit()

class Database:
# self.tables - array of table objects from ETL Model
# self.tableMap - hash map (name:Table) of table objects from ETL Model
	def __init__(self, node):
		self.tables = []
		self.tableMap = {}
	def toXML(self):
		xml = self.startElement()
		for tbl in self.tables:
			xml += tbl.toXML()
		xml += self.endElement()
		return xml
	def startElement(self):
		return '<ETL1>'
	def endElement(self):
		return '\n</ETL1>'
	def setLPTSValues(self, lptsMap): # This could belong with Reader
		for tbl in self.tables:
			for col in tbl.columns:
				for parm in col.parameters:
					if parm.type == "lptsXML":
						parm.value = lptsMap.get(parm.name)
						#print "set LPTS", parm.name, parm.value


class Table:
# self.name - Table name
# self.pkey - Array of column names that make up primary key
# self.columns - Array of column objects from ETL Model
# self.columnMap - hash map (name:Column) of column objects from ETL Model
	def __init__(self, database, node):
		checkElementName(["table"], node)
		self.database = database
		self.name = node.getAttribute("name")
		keys = node.getAttribute("pkey").split(",")
		self.pkey = [key.strip() for key in keys]
		self.columns = []
		self.columnMap = {}
		database.tables.append(self)
		database.tableMap[self.name] = self
	def toXML(self):
		xml = self.startElement()
		for col in self.columns:
			xml += col.toXML()
		xml += self.endElement()
		return xml
	def startElement(self):
		xml = '\n\t<table name="%s"' % (self.name)
		if self.pkey != None:
			xml += ' pkey="%s"' % (", ".join(self.pkey))
		xml += '>'
		return xml
	def endElement(self):
		return '\n\t</table>'
	def insertSQL(self):
		sqlList = []
		for index in range(len(self.columns[0].values)):
			sql = "REPLACE INTO `%s` (" % (self.name)
			names = ["`" + col.name + "`" for col in self.columns]
			sql += ", ".join(names)
			sql += ") VALUES ("
			values = []
			for col in self.columns:
				idx = index
				if len(col.values) == 1:
					idx = 0
				val = "'" + col.values[idx] + "'" if col.values[idx] != None else "null"
				values.append(val)
			sql += ", ".join(values)
			sql += ")"
			sqlList.append(sql)
		return sqlList


class Column:
# self.name - Column name
# self.transform - Name of function to be used to transform data to column requirements
# self.parameters - Array of parameter objects from ETL Model
# ??? Should there be a parameter map, currently, they do not all have a name
# self.values - Array of transformed values for column; one for each pkey value
	def __init__(self, table, node):
		checkElementName(["column"], node)
		self.table = table
		self.name = node.getAttribute("name")
		self.transform = node.getAttribute("transform") if node.hasAttribute("transform") else None
		self.parameters = []
		self.parameterMap = {}
		self.values = []
		table.columns.append(self)
		table.columnMap[self.name] = self
	def getParamValue(self, parm):
		if isinstance(parm, int):
			par = self.parameters[parm]
		else:
			par = self.parameterMap[parm]
		if par.type != "variable":
			return par.value
		else:
			print "ERROR: Use getVariable on parameter %s in %s.%s" % (parm, self.table.name, self.name)
			sys.exit()
	def getVariable(self, parm):
		if isinstance(parm, int):
			par = self.parameters[parm]
		else:
			par = self.parameterMap[parm]
		if par.type == "variable":		
			if par.table == None:
				return self.table.columnMap[par.name].values
			else:
				return self.table.database.tableMap[par.table].columnMap[par.name].values
		else:
			print "ERROR: Use getParamValue on parameter %s in %s.%s" % (parm, self.table.name, self.name)
	def toXML(self):
		xml = self.startElement()
		for param in self.parameters:
			xml += param.toXML()
		xml += self.endElement()
		return xml
	def startElement(self):
		xml = '\n\t\t<column name="%s"' % (self.name)
		if self.transform != None:
			xml += ' transform="%s"' % (self.transform)
		xml += '>'
		return xml
	def endElement(self):
		return '\n\t\t</column>'

class Parameter:
# self.type - The element type of the parameter from the ETL Model
# self.name - The parameter name from the ETL Model ??? Should this exist for all types ???
# self.validate - The validation function from the ETL Model for this parameter
# self.clean - The clean function from the ETL Model for this parameter
# self.value - The value of the parameter as taken from the source
	def __init__(self, column, node):
		checkElementName(["lptsXML", "constant", "variable", "config", "sql", "external"], node)
		self.column = column
		self.type = node.nodeName
		self.name = node.getAttribute("name")
		self.table = None
		self.validate = None
		self.clean = None
		self.value = None
		if self.type == "lptsXML":
			self.validate = node.getAttribute("validate")
			self.clean = node.getAttribute("clean")
		elif self.type == "constant":
			self.value = node.firstChild.nodeValue
		elif self.type == "variable":
			self.table = node.getAttribute("table") if node.hasAttribute("table") else None
		elif self.type == "sql":
			self.value = node.firstChild.nodeValue
		elif self.type not in ["config", "external"]:
			print "invalid type %s" % (self.type)
			sys.exit()
		column.parameters.append(self)
		column.parameterMap[self.name] = self
	def toXML(self):
		xml = self.startElement()
		if self.value != None:
			xml += '>'
			xml += self.value
			xml += self.endElement()
		else:
			xml += '/>'
		return xml
	def startElement(self):
		xml = '\n\t\t\t<%s' % (self.type)
		if self.name != None:
			xml += ' name="%s"' % (self.name)
		if self.validate != None:
			xml += ' validate="%s"' % (self.validate)
		if self.clean != None:
			xml += ' clean="%s"' % (self.clean)
		#xml += '>'
		return xml
	def endElement(self):
		return '</%s>' % (self.type)


