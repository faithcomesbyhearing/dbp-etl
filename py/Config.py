# Config.py
#
# This program reads the configuration file in the HOME directory. 
# The file contains numerous configurations that are each labeled
# with a name in brackets as in [name]
# The intent is that a user might have more than one profile, such as
# dev, test, stage, and prod
#
#

import os
import sys
from pathlib import Path

# TODO: change to ~/.dbp-etl
CONFIG_FILE = os.path.join(str(Path.home()) , "fcbh_dbp.cfg")

class Config:

	def __init__(self):
		if not os.path.exists(CONFIG_FILE):
			print("ERROR: Config file '%s' does not exist." % (CONFIG_FILE))
			sys.exit()

		if len(sys.argv) < 2:
			print("ERROR: config profile, such as 'dev,test,prod' is first required parameter.")
			sys.exit()

		self.hashMap = {}
		profile = sys.argv[1]
		profileLabel = "[" + profile + "]"
		insideProfile = False
		cfg = open(CONFIG_FILE, "r")
		for line in cfg:
			line = line.strip()
			if not line.startswith("#"): # Comment
				if insideProfile:
					if line.startswith("["):
						break;
					parts = line.split("=")
					if len(parts) == 2:
						self.hashMap[parts[0].strip()] = parts[1].strip()
				elif line == profileLabel:
					insideProfile = True
		cfg.close()

		if len(self.hashMap) == 0:
			print("ERROR: Config profile %s does not exist in '%s'." % (profileLabel, CONFIG_FILE))
			sys.exit()

		programRunning = sys.argv[0].split(os.sep)[-1]

		self.database_host = self._get("database.host")
		self.database_user = self._get("database.user")
		self.database_passwd = self._get("database.passwd")
		self.database_db_name = self._get("database.db_name")
		self.database_port = self._getInt("database.port")

		self.s3_bucket = self._get("s3.bucket")
		self.s3_vid_bucket = self._get("s3.vid_bucket")
		self.s3_aws_profile = self._get("s3.aws_profile")

		if programRunning in {"Validate.py"}:

			self.directory_bucket_list = self._getPath("directory.bucket_list")
			self.filename_lpts_xml = self._getPath("filename.lpts_xml")

			self.directory_validate	= self._getPath("directory.validate")
			self.directory_upload	= self._getPath("directory.upload")
			self.directory_database	= self._getPath("directory.database")
			self.directory_complete	= self._getPath("directory.complete")

			self.directory_quarantine = self._getPath("directory.quarantine")
			self.directory_duplicate = self._getPath("directory.duplicate")
			self.directory_accepted = self._getPath("directory.accepted")

			self.directory_errors = self._getPath("directory.errors")
			self.error_limit_pct = self._getFloat("error.limit.pct")
			self.filename_accept_errors = self._getPath("filename.accept.errors")

			self.filename_datetime = self._get("filename.datetime")

			#self.permission_public_domain = self.get("permission.public_domain")
			#self.permission_fcbh_general = self.get("permission.fcbh_general")
			#self.permission_fcbh_web = self.get("permission.fcbh_web")
			#self.permission_fcbh_general_exclude = self.get("permission.fcbh_general_exclude")
			#self.permission_dbs_general = self.get("permission.dbs_general")
			#self.permission_restricted = self.get("permission.restricted")
			#self.permission_gideons_hide = self.get("permission.gideons_hide")
			#self.permission_fcbh_hub = self.get("permission.fcbh_hub")
			#self.permission_bibleis_hide = self.get("permission.bibleis_hide")

		elif programRunning in {"AudioHLS.py"}:

			self.directory_audio_hls = self._getPath("directory.audio_hls") #"%s/FCBH/files/tmp" % (os.environ["HOME"])
			self.audio_hls_duration_limit = self._getInt("audio.hls.duration.limit") #10  #### Must become command line param


	def _get(self, name):
		value = self.hashMap.get(name)
		if value == None:
			print("ERROR: Config entry '%s' is missing." % (name))
			sys.exit()
		return value

	def _getPath(self, name):
		value = self._get(name)
		path = value.replace("/", os.path.sep)
		if not os.path.exists(path):
			print("ERROR: path %s for %s does not exist" % (path, name))
			sys.exit()
		return path

	def _getInt(self, name):
		return int(self._get(name))

	def _getFloat(self, name):
		return float(self._get(name))



"""
# Unit Test
config = Config()
print config.database_user
print config.database_db_name
"""


