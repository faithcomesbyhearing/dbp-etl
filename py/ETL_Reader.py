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

ETL_MODEL_PATH = os.path.join("input", "ETL_Model.xml")
LPTS_EXTRACT_FILE = os.path.join("input", "qry_dbp4_Regular_and_NonDrama_10rec.xml")
CONFIG_FILE = os.path.join("input", "config.xml")
SQL_OUTPUT = os.path.join("output", "output.sql")

def ETLModelReader():
	doc = minidom.parse(ETL_MODEL_PATH)
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

def ConfigReader():
	result = {}
	doc = minidom.parse(CONFIG_FILE)
	for root in doc.childNodes:
		#if root.nodeType == 1:
		for fldNode in root.childNodes:
			if fldNode.nodeType == 1:
				print fldNode.nodeName


def LPTSExtractReader():
	resultSet = []
	doc = minidom.parse(LPTS_EXTRACT_FILE)
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

config = ConfigReader()
print config



database = ETLModelReader()
#print(database.toXML())
transform = Transform(database)
cleaner = Clean(database)

resultSet = LPTSExtractReader()
#print "Length extract", len(resultSet)
sqlOutput = io.open(SQL_OUTPUT, mode="w", encoding="utf-8")
for row in resultSet:
	database.setLPTSValues(row)
	cleaner.process()
	sqlResult = transform.process()
	for sql in sqlResult:
		sqlOutput.write("%s\n" % (sql))
	#for key in row.keys():
		#print "%s -> %s" % (key, row[key])
sqlOutput.close()


"""
config file elements
database.host
database.user
database.password
database.db_name
database.db_text
database.port
s3.bucket
s3.vid_bucket
s3.aws_profile
permissions.access_restricted
permissions.access_granted
permissions.access_video
directory.upload_path
directory.lpts_xml
"""


