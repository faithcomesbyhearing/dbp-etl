# Config.py
#
# This program reads the config.xml file when instantiated, and contains members for the
# config.xml file entries
#

import os
import sys
from xml.dom import minidom

CONFIG_FILE = os.path.join("input", "config.xml")

class Config:

	def __init__(self):
		self.hashMap = {}
		doc = minidom.parse(CONFIG_FILE)
		for root in doc.childNodes:
			if root.nodeType == 1:
				for sectNode in root.childNodes:
					if sectNode.nodeType == 1:
						for fldNode in sectNode.childNodes:
							if fldNode.nodeType == 1:
								name = sectNode.nodeName + "." + fldNode.nodeName
								self.hashMap[name] = fldNode.firstChild.nodeValue

		self.database_host = self.get("database.host")
		self.database_user = self.get("database.user")
		self.database_input_db_name = self.get("database.input_db_name")
		self.database_output_db_name = self.get("database.output_db_name")
		self.database_port = self.getInt("database.port")
		self.directory_lpts_xml = self.getPath("directory.lpts_xml")
		self.directory_main_bucket = self.getPath("directory.main_bucket")
		self.directory_sql_output = self.getPath("directory.sql_output")

	def get(self, name):
		value = self.hashMap.get(name)
		if value == None:
			print("ERROR: Config entry '%s' is missing." % (name))
			sys.exit()
		return value

	def getPath(self, name):
		value = self.get(name)
		parts = value.split("/")
		return os.path.sep.join(parts)

	def getInt(self, name):
		return int(self.get(name))



"""
# Unit Test
config = Config()
print config
"""
