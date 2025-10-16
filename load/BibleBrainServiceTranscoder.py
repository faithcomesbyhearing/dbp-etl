import json
import time
from RunStatus import RunStatus
from AWSSession import AWSSession
from Log import Log

class BibleBrainServiceTranscoder:
	def __init__(self, config, filesetPrefix):
		self.config = config
		self.filesetPrefix = filesetPrefix
		self.openJobs = []
		# ECS-specific attributes
		self.ecsClient = None
		self.openTasks = []
		self.hlsSegmentDuration = 10 # seconds
		self.resolutionsAvailableToTranscode = ["720p", "480p", "360p"]


	@staticmethod
	def transcodeVideoFilesetECS(config, filesetPrefix, s3FileKeys, sourceBucket=None, filesetPath=None):
		"""Transcode video fileset using ECS tasks instead of Elastic Transcoder.

		Args:
			config: Configuration object
			filesetPrefix: The fileset prefix identifier
			s3FileKeys: List of S3 file keys to transcode

		Returns:
			bool: True if all ECS tasks completed successfully, False otherwise.
		"""
		transcoder = BibleBrainServiceTranscoder(config, filesetPrefix)
		RunStatus.printDuration("BEGIN SUBMIT ECS TRANSCODE")
		createdTask = transcoder.createECSTask(s3FileKeys, sourceBucket, filesetPath)
		RunStatus.printDuration("BEGIN CHECK ECS TRANSCODE")
		if createdTask is None:
			Log.getLogger(filesetPrefix).message(Log.FATAL, "ERROR: Transcode of %s in %s failed to run ECS Task." % (filesetPrefix, sourceBucket))
			return False
		else:
			print("ECS Transcode %s started: %s" % (filesetPrefix, createdTask))

		done = transcoder.completeECSTasks()
		if done:
			print("ECS Transcode %s succeeded." % (filesetPrefix))
		else:
			print("ECS Transcode %s FAILED." % (filesetPrefix))
		RunStatus.printDuration("ECS TRANSCODE DONE")

		return done


	def createECSTask(self, s3FileKeys, sourceBucket, filesetPath):
		"""Creates an ECS task to transcode a batch of video files.

		Args:
			s3FileKeys: List of S3 file keys to transcode

		Returns:
			The task ARN if successful, None otherwise
		"""
		# Initialize ECS client if not already done
		if self.ecsClient is None:
			self.ecsClient = AWSSession.shared().ecsClient()

		# Parse bucket and prefix from the first file key
		# Expected format: "upload-bucket/prefix/filename.mp4"
		if not s3FileKeys:
			print("ERROR: No files provided for transcoding")
			return None

		# Extract input bucket, prefix, and filenames
		firstKey = s3FileKeys[0]
		print("DEBUG: First S3 key: %s" % firstKey)
		parts = firstKey.split("/")
		if len(parts) < 2:
			print("ERROR: Invalid S3 key format: %s" % firstKey)
			return None

		# inputBucket = self.config.s3_vid_bucket
		# inputPrefix = "/".join(parts[:-1])
		inputBucket = sourceBucket
		inputPrefix = filesetPath
		inputFiles = [key.split("/")[-1] for key in s3FileKeys]
		print("DEBUG: Parsed inputBucket=%s, inputPrefix=%s" % (inputBucket, inputPrefix))
		print("DEBUG: Number of input files: %d" % len(inputFiles))

		# Construct output bucket and prefix
		outputBucket = self.config.s3_vid_bucket
		# Extract the fileset ID (e.g., "ENGESVP2DV" from "video/ENGESV/ENGESVP2DV")
		# filesetPrefix is already the full path like "video/ENGESV/ENGESVP2DV"
		outputPrefix = self.filesetPrefix

		# Define output resolutions with HLS segment duration
		outputs = [
			{
				"resolution": res,
				"hlsSegmentDuration": self.hlsSegmentDuration
			} for res in self.resolutionsAvailableToTranscode
		]

		# Build the task input payload
		taskInput = {
			"inputBucket": inputBucket,
			"inputPrefix": inputPrefix,
			"inputFiles": inputFiles,
			"outputBucket": outputBucket,
			"outputPrefix": outputPrefix,
			"outputs": outputs
		}

		print("========================================================================")
		print("========================================================================")
		print("ecs_cluster: %s" % self.config.transcoder_ecs_cluster_name)
		print("ecs_task_definition: %s" % self.config.transcoder_ecs_task_definition)
		print("ecs_container_name: %s" % self.config.transcoder_ecs_container_name)
		# print("security_group: %s" % self.config.transcoder_ecs_placement_security_group)
		# print("subnets: %s" % self.config.transcoder_ecs_placement_subnet)
		# print("")
		# print("Creating ECS task with input: %s" % json.dumps(taskInput, indent=2))
		print("========================================================================")
		print("========================================================================")
		# Run the ECS task
		try:
			response = self.ecsClient.run_task(
				cluster=self.config.transcoder_ecs_cluster_name,
				taskDefinition="biblebrain-services-transcoder-dev",
				launchType='FARGATE',
				networkConfiguration={
					'awsvpcConfiguration': {
						'subnets': [self.config.transcoder_ecs_placement_subnet],
						'securityGroups': [self.config.transcoder_ecs_placement_security_group],
						'assignPublicIp': 'ENABLED'
					}
				},
				overrides={
					'containerOverrides': [
						{
							'name': self.config.transcoder_ecs_container_name,
							'command': [
								json.dumps(taskInput)
							]
						}
					]
				}
			)

			if response.get('failures'):
				print("ERROR: Failed to create ECS task:")
				for failure in response['failures']:
					print("  - %s: %s" % (failure.get('reason'), failure.get('detail')))
				return None

			tasks = response.get('tasks', [])
			if not tasks:
				print("ERROR: No tasks created")
				return None

			taskArn = tasks[0]['taskArn']
			print("ECS task created: %s" % taskArn)
			self.openTasks.append(taskArn)
			return taskArn

		except Exception as e:
			print("ERROR creating ECS task: %s" % str(e))
			return None


	def completeECSTasks(self):
		"""Monitors ECS tasks until completion.

		Returns:
			True if all tasks completed successfully, False otherwise
		"""
		# Initialize ECS client if not already done
		if self.ecsClient is None:
			self.ecsClient = AWSSession.shared().ecsClient()

		errorCount = 0
		successCount = 0

		while len(self.openTasks) > 0:
			stillOpenTasks = []

			# Describe tasks to get their status
			try:
				response = self.ecsClient.describe_tasks(
					cluster=self.config.transcoder_ecs_cluster_name,
					tasks=self.openTasks
				)

				for task in response.get('tasks', []):
					taskArn = task['taskArn']
					lastStatus = task.get('lastStatus')

					if lastStatus == 'STOPPED':
						# Check exit code
						containers = task.get('containers', [])
						taskFailed = False

						# Capture task-level stop information
						taskStoppedReason = task.get('stoppedReason', 'No reason provided')
						taskStopCode = task.get('stopCode', 'Unknown')

						for container in containers:
							exitCode = container.get('exitCode')
							containerName = container.get('name', 'unknown')

							print("DEBUG: Container '%s' exitCode=%s" % (containerName, exitCode))

							if exitCode is not None and exitCode != 0:
								print("ERROR: Task %s failed with exit code %s" % (taskArn, exitCode))
								print("  Container Name: %s" % containerName)
								print("  Reason: %s" % container.get('reason', 'Unknown'))
								print("  Container Stopped Reason: %s" % container.get('stoppedReason', 'Not available'))
								print("  Task Stop Code: %s" % taskStopCode)
								print("  Task Stopped Reason: %s" % taskStoppedReason)
								taskFailed = True
								errorCount += 1
							elif exitCode == 0:
								print("Task Complete: %s (exit code: 0)" % taskArn)
								successCount += 1
							else:
								print("WARNING: Task %s container '%s' has exitCode=None" % (taskArn, containerName))
								print("  Task Stop Code: %s" % taskStopCode)
								print("  Task Stopped Reason: %s" % taskStoppedReason)
								print("  Container Stopped Reason: %s" % container.get('stoppedReason', 'Not available'))

						if not taskFailed and not containers:
							print("WARNING: Task %s completed but has no containers" % taskArn)
							print("  Task Stop Code: %s" % taskStopCode)
							print("  Task Stopped Reason: %s" % taskStoppedReason)

					elif lastStatus in ['PENDING', 'RUNNING', 'PROVISIONING', 'DEPROVISIONING']:
						stillOpenTasks.append(taskArn)
						print("Task %s status: %s" % (taskArn, lastStatus))

					else:
						# Unexpected status
						print("WARNING: Task %s has unexpected status: %s" % (taskArn, lastStatus))
						stillOpenTasks.append(taskArn)

			except Exception as e:
				print("ERROR checking task status: %s" % str(e))
				errorCount += 1

			self.openTasks = stillOpenTasks

			if len(self.openTasks) > 0:
				time.sleep(10)

		print("DEBUG: completeECSTasks finished - successCount=%d, errorCount=%d" % (successCount, errorCount))
		return errorCount == 0
