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
			self.home = os.getcwd()

		configFile = os.path.join(self.home, "dbp-etl.cfg")
		if not os.path.exists(configFile):
			print("ERROR: Config file '%s' does not exist." % (configFile))
			sys.exit()

		profile = os.environ.get('PROFILE', "")
		if (len(profile)) ==0:
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
		self.profile = profile
		self.hashMap = config[profile]

		#for key, value in self.hashMap.items():
		#	print(key, value)

		if len(self.hashMap) == 0:
			print("ERROR: Config profile %s does not exist in '%s'." % (profileLabel, configFile))
			sys.exit()

		splitPattern = re.compile("\\\\|/") # I have no idea why \\\\ escapes to one \
		programRunning = splitPattern.split(sys.argv[0])[-1]

		self.database_names = {}
		self.current_database_name = None
		self.setConfigParametersFromProfile(profile, programRunning)

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

	def getDatabaseNames(self):
		return self.database_names

	def setConfigParametersFromProfile(self, profile, programRunning):
		print("Config:setConfigParametersFromProfile. profile: %s, programRunning: %s" % (profile, programRunning))
		self.s3_artifacts_bucket = self._get("s3.artifacts_bucket")
		self.s3_aws_profile = self._getOptional("s3.aws_profile") 
		self.s3_aws_region = self._getOptional("s3.aws_region")
		self.s3_aws_role_arn = self._getOptional("s3.aws_role_arn") 
		self.s3_aws_role_profile = self._getOptional("s3.aws_role_profile") # this is temporary
		if os.environ.get('DATA_MODEL_MIGRATION_STAGE') == 'B':
			self.filename_lpts_xml = self._getOptional("filename.lpts_xml")
		self.filename_metadata_xml = self._getOptional("filename.metadata_xml")
		self.s3_bucket = self._get("s3.bucket")
		self.s3_vid_bucket = self._get("s3.vid_bucket")
		self.directory_accepted = self._getOptional("directory.accepted")
		self.directory_errors = self._getOptional("directory.errors")
		self.directory_upload_aws = self._getOptional("directory.upload_aws")
		self.directory_quarantine = self._getOptional("directory.quarantine")
		self.directory_duplicate = self._getOptional("directory.duplicate")
		self.directory_bucket_list = self._getOptional("directory.bucket_list")
		self.node_exe = self._getOptional("node.exe")
		self.sofria_client_js = self._getOptional("sofria_client.js")
		self.filename_accept_errors = self._getOptional("filename.accept.errors")
		self.filename_datetime = self._get("filename.datetime")
		self.mysql_exe = self._getOptional("mysql.exe")
		self.data_missing_verses_allowed = self._getOptional("data.missing_verses_allowed")
		self.s3_zipper_user_key = self._getOptional("s3.zipper.user_key")
		self.s3_zipper_user_secret = self._getOptional("s3.zipper.user_secret")
		self.biblebrain_services_base_url = self._getOptional("biblebrain.services.base_url")
		self.cdn_partner_base = self._getOptional("cdn.partner_base")
		self.monday_completed_product_code_api_key = self._getOptional("monday.completed_product_code.api_key")
		self.monday_completed_product_code_board_id = self._getOptional("monday.completed_product_code.board_id")

		# TODO these dependencies need to be sorted out
		if programRunning in {"DBPLoadController.py"}:
			self.audio_transcoder_url = self._get("audio.transcoder.url")
			self.audio_transcoder_key = self._get("audio.transcoder.key")
			self.audio_transcoder_sleep_sec = self._getInt("audio.transcoder.sleep.sec")
			self.audio_transcoder_input = self._get("audio.transcoder.input")
			self.lambda_zip_function = self._get("lambda.zip.function")
			self.lambda_zip_region = self._get("lambda.zip.region")
			self.lambda_zip_timeout = self._getInt("lambda.zip.timeout")

			self.video_transcoder_region = self._get("video.transcoder.region")
			self.video_transcoder_pipeline = self._get("video.transcoder.pipeline")
			self.video_preset_hls_1080p = self._get("video.preset.hls.1080p")
			self.video_preset_hls_720p = self._get("video.preset.hls.720p")
			self.video_preset_hls_480p = self._get("video.preset.hls.480p")
			self.video_preset_hls_360p = self._get("video.preset.hls.360p")
			self.video_preset_web = self._get("video.preset.web")

			# ECS-based video transcoding configuration
			self.setTranscoderECSParameters()

			self.database_names['dbp'] = self.hashMap.get("database.db_name")
			self.database_names['user_dbp'] = self.hashMap.get("database.user_db_name")
			self.database_host = self._get("database.host")
			self.database_user = self._get("database.user")
			self.database_passwd = self._get("database.passwd")
			self.database_db_name = self._get("database.db_name")
			self.database_user_db_name = self._get("database.user_db_name")
			self.database_port = self._getInt("database.port")
			self.database_tunnel = self._getOptional("database.tunnel")

		elif programRunning in {"AudioHLS.py"}:
			self.directory_audio_hls = self._getPath("directory.audio_hls") #"%s/FCBH/files/tmp" % (self.home)
			self.audio_hls_duration_limit = self._getInt("audio.hls.duration.limit") #10  #### Must become command line param

		if profile == 'test':
			# video
			self.video_transcoder_pipeline = self._get("video.transcoder.pipeline")
			self.video_transcoder_region = self._get("video.transcoder.region")
			self.video_preset_hls_1080p = self._get("video.preset.hls.1080p")
			self.video_preset_hls_720p = self._get("video.preset.hls.720p")
			self.video_preset_hls_480p = self._get("video.preset.hls.480p")
			self.video_preset_hls_360p = self._get("video.preset.hls.360p")
			self.video_preset_web = self._get("video.preset.web")

			# ECS-based video transcoding configuration
			self.setTranscoderECSParameters()

			# audio
			self.audio_transcoder_sleep_sec = self._getInt("audio.transcoder.sleep.sec")
			self.audio_transcoder_input = self._get("audio.transcoder.input")

		if profile in {'test', 'dev'}:
			self.video_transcoder_url = self._getOptional("video.transcoder.url")
			self.database_names['dbp'] = self.hashMap.get("database.db_name")
			self.database_names['user_dbp'] = self.hashMap.get("database.user_db_name")
			self.database_host = self._get("database.host")
			self.database_user = self._get("database.user")
			self.database_passwd = self._get("database.passwd")
			self.database_db_name = self._get("database.db_name")
			self.database_user_db_name = self._get("database.user_db_name")
			self.database_port = self._getInt("database.port")
			self.database_tunnel = self._getOptional("database.tunnel")
			self.setCurrentDatabaseDBName(self.hashMap.get("database.db_name"))
		elif profile == 'linguasource':
			self.database_host = self._get("database.host")
			self.database_user = self._get("database.user")
			self.database_passwd = self._get("database.passwd")
			self.language_db_name = self._get("database.language_db_name")
			self.biblebrain_db_name = self._get("database.biblebrain_db_name")
			self.database_port = self._getInt("database.port")
			self.database_tunnel = self._getOptional("database.tunnel")
			self.database_names['language'] = self.hashMap.get("database.language_db_name")
			self.database_names['biblebrain'] = self.hashMap.get("database.biblebrain_db_name")
			self.setCurrentDatabaseDBName(self.hashMap.get("database.language_db_name"))
		elif profile == 'newdata':
			self.database_names['dbp'] = self.hashMap.get("database.db_name")
			self.database_names['user_dbp'] = self.hashMap.get("database.user_db_name")
			self.database_host = self._get("database.host")
			self.database_user = self._get("database.user")
			self.database_passwd = self._get("database.passwd")
			self.database_db_name = self._get("database.db_name")
			self.database_user_db_name = self._get("database.user_db_name")
			self.database_port = self._getInt("database.port")
			self.database_tunnel = self._getOptional("database.tunnel")
			self.setCurrentDatabaseDBName(self.hashMap.get("database.db_name"))

	def setTranscoderECSParameters(self):
		# ECS-based video transcoding configuration
		self.transcoder_ecs_region = self._get("transcoder.ecs_region")
		self.transcoder_ecs_cluster_name = self._get("transcoder.ecs_cluster_name")
		self.transcoder_ecs_container_name = self._get("transcoder.ecs_container_name")
		self.transcoder_ecs_placement_security_group = self._get("transcoder.ecs_placement_security_group")
		self.transcoder_ecs_placement_subnet = self._get("transcoder.ecs_placement_subnet")
		self.transcoder_ecs_task_definition = self._get("transcoder.ecs_task_definition")
		self.transcoder_ecs_disabled = False if self._getOptional("transcoder.ecs_disabled") is None else self._getOptional("transcoder.ecs_disabled") in {'1', 'true', 'True', 'TRUE'}

	def setCurrentDatabaseDBName(self, name):
		self.current_database_name = name

	def getCurrentDatabaseDBName(self):
		return self.current_database_name

# Unit Test
if (__name__ == '__main__'):
	config = Config()
	print("User", config.database_user)
	# print("DB", config.database_db_name)
	print("Current DB name", config.current_database_name)
	print("DB names", config.getDatabaseNames())
