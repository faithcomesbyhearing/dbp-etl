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
		self.s3_bucket = self.get("s3.bucket")
		self.s3_vid_bucket = self.get("s3.vid_bucket")
		self.s3_aws_profile = self.get("s3.aws_profile")
		self.permission_public_domain = self.get("permission.public_domain")
		self.permission_fcbh_general = self.get("permission.fcbh_general")
		self.permission_fcbh_web = self.get("permission.fcbh_web")
		self.permission_fcbh_general_exclude = self.get("permission.fcbh_general_exclude")
		self.permission_dbs_general = self.get("permission.dbs_general")
		self.permission_restricted = self.get("permission.restricted")
		self.permission_gideons_hide = self.get("permission.gideons_hide")
		self.permission_fcbh_hub = self.get("permission.fcbh_hub")
		self.permission_bibleis_hide = self.get("permission.bibleis_hide")
		self.directory_lpts_xml = self.getPath("directory.lpts_xml")
		self.directory_bucket_list = self.getPath("directory.bucket_list")
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
