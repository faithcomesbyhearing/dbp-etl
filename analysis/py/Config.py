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
		self.database_db_name = self.get("database.db_name")
		self.database_port = self.getInt("database.port")
		self.s3_bucket = self.get("s3.bucket")
		self.s3_vid_bucket = self.get("s3.vid_bucket")
		self.s3_aws_profile = self.get("s3.aws_profile")
		self.permissions_restricted = self.get("permissions.access_restricted")
		self.permissions_granted = self.get("permissions.access_granted")
		self.permissions_video = self.get("permissions.access_video")
		self.directory_etl_model_xml = self.getPath("directory.etl_model_xml")
		self.directory_lpts_xml = self.getPath("directory.lpts_xml")
		self.directory_validate = self.getPath("directory.validate")
		self.directory_upload = self.getPath("directory.upload")
		self.directory_database = self.getPath("directory.database")
		self.directory_complete = self.getPath("directory.complete")
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
