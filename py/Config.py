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
import re
import boto3
import configparser

class Config:

	_instance = None
	def shared():
		if Config._instance == None:
			Config._instance = Config()
		return Config._instance


	def __init__(self):
		self.home = os.environ.get('HOME') # unix
		if self.home == None:
			self.home = os.environ.get('HOMEPATH') # windows
		if self.home == None:
			print("ERROR: Environment variable HOME or HOMEPATH must be set to a directory.")
			sys.exit()

		configFile = os.path.join(self.home, "dbp-etl.cfg")
		if not os.path.exists(configFile):
			print("ERROR: Config file '%s' does not exist." % (configFile))
			sys.exit()

		if len(sys.argv) < 2:
			print("ERROR: config profile, such as 'dev,test,prod' is first required parameter.")
			sys.exit()
		profile = sys.argv[1]

		config = configparser.ConfigParser(interpolation = None)
		config.read(configFile)
		sections = config.sections()
		if profile not in sections:
			print("ERROR: config profile %s is not in %s" % (profile, configFile))
			sys.exit()
		self.hashMap = config[profile]

		#for key, value in self.hashMap.items():
		#	print(key, value)

		if len(self.hashMap) == 0:
			print("ERROR: Config profile %s does not exist in '%s'." % (profileLabel, configFile))
			sys.exit()

		splitPattern = re.compile("\\\\|/") # I have no idea why \\\\ escapes to one \
		programRunning = splitPattern.split(sys.argv[0])[-1]

		#self.node_exe = self._getPath("node.exe")
		#self.mysql_exe = self._getPath("mysql.exe")
		self.database_host = self._get("database.host")
		self.database_user = self._get("database.user")
		self.database_passwd = self._get("database.passwd")
		self.database_db_name = self._get("database.db_name")
		self.database_user_db_name = self._get("database.user_db_name")
		self.database_port = self._getInt("database.port")
		self.database_tunnel = self._getOptional("database.tunnel")

		self.s3_bucket = self._get("s3.bucket")
		self.s3_vid_bucket = self._get("s3.vid_bucket")
		self.s3_artifacts_bucket = self._get("s3.artifacts_bucket")
		self.s3_aws_profile = self._getOptional("s3.aws_profile")
		self.s3_aws_role_arn = self._getOptional("s3.aws_role_arn")
		# self.s3_aws_role_profile = self._getOptional("s3.aws_role_profile") # this is temporary

		#if programRunning in {"AudioHLS.py"}:
		#	self.directory_audio_hls = self._getPath("directory.audio_hls") #"%s/FCBH/files/tmp" % (self.home)
		#	self.audio_hls_duration_limit = self._getInt("audio.hls.duration.limit") #10  #### Must become command line param

		#else:
		#	self.audio_transcoder_url = self._get("audio.transcoder.url")
		#	self.audio_transcoder_key = self._get("audio.transcoder.key")
		#	self.audio_transcoder_sleep_sec = self._getInt("audio.transcoder.sleep.sec")
		#	self.audio_transcoder_input = self._get("audio.transcoder.input")

		#	self.video_transcoder_region = self._get("video.transcoder.region")
		#	self.video_transcoder_pipeline = self._get("video.transcoder.pipeline")
		#	self.video_preset_hls_1080p = self._get("video.preset.hls.1080p")
		#	self.video_preset_hls_720p = self._get("video.preset.hls.720p")
		#	self.video_preset_hls_480p = self._get("video.preset.hls.480p")
		#	self.video_preset_hls_360p = self._get("video.preset.hls.360p")
		#	self.video_preset_web = self._get("video.preset.web")
		#	self.video_aws_profile = self._getOptional("video.aws_profile")

		#	self.directory_bucket_list = self._getPath("directory.bucket_list")
		#	self.filename_lpts_xml = self._getPath("filename.lpts_xml")
		#
		#	self.directory_upload_aws = self._getPath("directory.upload_aws")
		#	#self.directory_upload = self._getPath("directory.upload")
		#	#self.directory_database	= self._getPath("directory.database")
		#	#self.directory_complete	= self._getPath("directory.complete")

		#	self.directory_quarantine = self._getPath("directory.quarantine")
		#	self.directory_duplicate = self._getPath("directory.duplicate")
		#	self.directory_accepted = self._getPath("directory.accepted")

		#	self.directory_errors = self._getPath("directory.errors")
		#	self.filename_accept_errors = self._getPath("filename.accept.errors")

		#	self.filename_datetime = self._get("filename.datetime")

		print("Config '%s' is loaded." % (profile))


	def _get(self, name):
		value = self.hashMap.get(name)
		if value == None:
			print("ERROR: Config entry '%s' is missing." % (name))
			sys.exit()
		return value

	def _getPath(self, name):
		value = self._get(name)
		path = value.replace("/", os.path.sep)
		if path.startswith("~"):
			path = path.replace("~", self.home)
		if not os.path.exists(path):
			print("ERROR: path %s for %s does not exist" % (path, name))
			sys.exit()
		return path

	def _getInt(self, name):
		return int(self._get(name))

	def _getFloat(self, name):
		return float(self._get(name))

	def _getOptional(self, name):
		return self.hashMap.get(name)

	def getOptional(self, name):
		return self.hashMap.get(name)


# Unit Test
if (__name__ == '__main__'):
	config = Config()
	print("User", config.database_user)
	print("DB", config.database_db_name)
