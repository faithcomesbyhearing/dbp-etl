# ETL_Reader
#
# This program reads the ETL_Model.xml file and builds a tree of objects
#

import io
import sys
import os
from xml.dom import minidom
from ETL_Model import Database, Table, Column, Parameter
from Transform import *
from Clean import *
from Config import *

def ETLModelReader(config):
	doc = minidom.parse(config.directory_etl_model_xml)
	for root in doc.childNodes:
		if root.nodeType == 1:
			database = Database(root)
			for tblNode in root.childNodes:
				if tblNode.nodeType == 1:
					table = Table(database, tblNode)
					for colNode in tblNode.childNodes:
						if colNode.nodeType == 1:
							column = Column(table, colNode)
							for parmNode in colNode.childNodes:
								if parmNode.nodeType == 1:
									param = Parameter(column, parmNode)
	return database

def LPTSExtractReader(config):
	resultSet = []
	doc = minidom.parse(config.directory_lpts_xml)
	root = doc.childNodes
	if len(root) != 1 or root[0].nodeName != "dataroot":
		print ("ERROR: First node of LPTS Export must be <docroot>")
		sys.exit()
	else:
		#print root[0].nodeName
		for recNode in root[0].childNodes:
			if recNode.nodeType == 1:
				if recNode.nodeName != "qry_dbp4_Regular_and_NonDrama":
					print "ERROR: Child nodes in LPTS Export must be 'qry_dbp4_Regular_and_NonDrama'"
					sys.exit()
				else:
					resultRow = {}
					for fldNode in recNode.childNodes:
						if fldNode.nodeType == 1:
							#print fldNode.nodeName + " = " + fldNode.firstChild.nodeValue
							resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue

					resultSet.append(resultRow)
	return resultSet

config = Config()

database = ETLModelReader(config)
#print(database.toXML())

transform = Transform(database)
cleaner = Clean(database)

resultSet = LPTSExtractReader(config)
#print "Length extract", len(resultSet)
sqlOutput = io.open(config.directory_sql_output, mode="w", encoding="utf-8")
for row in resultSet:
	database.setLPTSValues(row)
	cleaner.process()
	sqlResult = transform.process()
	for sql in sqlResult:
		sqlOutput.write("%s\n" % (sql))
	#for key in row.keys():
		#print "%s -> %s" % (key, row[key])
sqlOutput.close()





