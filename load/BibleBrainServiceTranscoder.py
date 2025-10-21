import json
import time
import os
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
		self.taskToFilesMap = {}
		self.hlsSegmentDuration = 10 # seconds
		self.resolutionsAvailableToTranscode = ["720p", "480p", "360p"]
		# Get batch size from environment variable, default to 1
		self.batchSize = int(os.environ.get('TRANSCODE_BATCH_SIZE_VIDEO', '1'))

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
		createdTasks = transcoder.createECSTasks(s3FileKeys, sourceBucket, filesetPath)
		RunStatus.printDuration("BEGIN CHECK ECS TRANSCODE")
		if 	createdTasks is None:
			Log.getLogger(filesetPrefix).message(Log.FATAL, "ERROR: Transcode of %s in %s failed to run ECS Tasks." % (filesetPrefix, sourceBucket))
			return False
		else:
			print("ECS Transcode %s started: %s" % (filesetPrefix, createdTasks))

		done = transcoder.completeECSTasks()
		if done:
			print("ECS Transcode %s succeeded." % (filesetPrefix))
		else:
			print("ECS Transcode %s FAILED." % (filesetPrefix))
		RunStatus.printDuration("ECS TRANSCODE DONE")

		return done


	def createECSTasks(self, s3FileKeys, sourceBucket, filesetPath):
		"""Creates ECS tasks to transcode video files in batches.

		Args:
			s3FileKeys: List of S3 file keys to transcode
			sourceBucket: Source S3 bucket name
			filesetPath: Path to the fileset in the bucket

		Returns:
			True if at least one task was created successfully, None otherwise
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

		inputBucket = sourceBucket
		inputPrefix = filesetPath
		inputFiles = [key.split("/")[-1] for key in s3FileKeys]

		print("DEBUG: Parsed inputBucket=%s, inputPrefix=%s" % (inputBucket, inputPrefix))
		print("DEBUG: Total number of input files: %d" % len(inputFiles))
		print("DEBUG: Batch size: %d" % self.batchSize)
		print("DEBUG: files:", inputFiles)

		# Construct output bucket and prefix
		outputBucket = self.config.s3_vid_bucket
		outputPrefix = self.filesetPrefix

		# Define output resolutions with HLS segment duration
		outputs = [
			{
				"resolution": res,
				"hlsSegmentDuration": self.hlsSegmentDuration
			} for res in self.resolutionsAvailableToTranscode
		]

		# Split input files into batches
		batches = []
		for i in range(0, len(inputFiles), self.batchSize):
			batch = inputFiles[i:i + self.batchSize]
			batches.append(batch)

		print("DEBUG: Number of batches to process: %d" % len(batches))

		print("========================================================================")
		print("========================================================================")
		print("ecs_cluster: %s" % self.config.transcoder_ecs_cluster_name)
		print("ecs_task_definition: %s" % self.config.transcoder_ecs_task_definition)
		print("ecs_container_name: %s" % self.config.transcoder_ecs_container_name)
		print("========================================================================")
		print("========================================================================")

		# Process each batch
		for batchIndex, batch in enumerate(batches):
			# Build the task input payload for this batch
			taskInput = {
				"inputBucket": inputBucket,
				"inputPrefix": inputPrefix,
				"inputFiles": batch,
				"outputBucket": outputBucket,
				"outputPrefix": outputPrefix,
				"outputs": outputs
			}

			print("Submitting ECS task for batch %d/%d with %d file(s): %s" %
				  (batchIndex + 1, len(batches), len(batch), batch))

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
					print("ERROR: Failed to create ECS task for batch %d:" % (batchIndex + 1))
					for failure in response['failures']:
						print("  - %s: %s" % (failure.get('reason'), failure.get('detail')))
					return None

				tasks = response.get('tasks', [])
				if not tasks:
					print("ERROR: No tasks created for batch %d" % (batchIndex + 1))
					return None

				taskArn = tasks[0]['taskArn']
				print("ECS task created for batch %d: %s" % (batchIndex + 1, taskArn))
				self.openTasks.append(taskArn)
				# Map task ARN to the files it's processing
				self.taskToFilesMap[taskArn] = batch

			except Exception as e:
				print("ERROR creating ECS task for batch %d: %s" % (batchIndex + 1, str(e)))
				return None

		print("DEBUG: Successfully created %d ECS tasks" % len(self.openTasks))
		return len(self.openTasks) > 0

	def completeECSTasks(self):
		"""Monitors ECS tasks until completion.

		Continues processing all tasks even if some fail, and reports which files failed.

		Returns:
			True if all tasks completed successfully, False otherwise
		"""
		# Initialize ECS client if not already done
		if self.ecsClient is None:
			self.ecsClient = AWSSession.shared().ecsClient()

		errorCount = 0
		failedFiles = []  # Track files from failed tasks
		successfulFiles = []  # Track files from successful tasks

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
					taskFiles = self.taskToFilesMap.get(taskArn, [])

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
								print("=" * 80)
								print("ERROR: Task %s failed with exit code %s" % (taskArn, exitCode))
								print("  Container Name: %s" % containerName)
								print("  Reason: %s" % container.get('reason', 'Unknown'))
								print("  Container Stopped Reason: %s" % container.get('stoppedReason', 'Not available'))
								print("  Task Stop Code: %s" % taskStopCode)
								print("  Task Stopped Reason: %s" % taskStoppedReason)
								print("  Files being processed by this task:")
								for fileName in taskFiles:
									print("    - %s" % fileName)
								print("=" * 80)
								taskFailed = True
								errorCount += 1
								# Add files to failed list
								failedFiles.extend(taskFiles)
							elif exitCode == 0:
								print("Task Complete: %s (exit code: 0)" % taskArn)
								print("  Files successfully processed: %s" % taskFiles)
								# Add files to successful list
								successfulFiles.extend(taskFiles)
							else:
								print("WARNING: Task %s container '%s' has exitCode=None" % (taskArn, containerName))
								print("  Task Stop Code: %s" % taskStopCode)
								print("  Task Stopped Reason: %s" % taskStoppedReason)
								print("  Container Stopped Reason: %s" % container.get('stoppedReason', 'Not available'))
								print("  Files being processed: %s" % taskFiles)

						if not taskFailed and not containers:
							print("WARNING: Task %s completed but has no containers" % taskArn)
							print("  Task Stop Code: %s" % taskStopCode)
							print("  Task Stopped Reason: %s" % taskStoppedReason)
							print("  Files being processed: %s" % taskFiles)

					elif lastStatus in ['PENDING', 'RUNNING', 'PROVISIONING', 'DEPROVISIONING']:
						stillOpenTasks.append(taskArn)
						print("Task %s status: %s (processing %d file(s))" % (taskArn, lastStatus, len(taskFiles)))

					else:
						# Unexpected status
						print("WARNING: Task %s has unexpected status: %s" % (taskArn, lastStatus))
						print("  Files being processed: %s" % taskFiles)
						stillOpenTasks.append(taskArn)

			except Exception as e:
				print("ERROR checking task status: %s" % str(e))
				# Continue processing other tasks instead of stopping
				print("Continuing to monitor remaining tasks...")

			self.openTasks = stillOpenTasks

			if len(self.openTasks) > 0:
				time.sleep(10)

		# Final summary
		print("=" * 80)
		print("TRANSCODING SUMMARY")
		print("=" * 80)
		print("Total files successfully processed: %d" % len(successfulFiles))
		print("Total files failed: %d" % len(failedFiles))

		if failedFiles:
			print("\nFailed files:")
			for fileName in failedFiles:
				print("  - %s" % fileName)

		if successfulFiles:
			print("\nSuccessful files:")
			for fileName in successfulFiles:
				print("  - %s" % fileName)

		print("=" * 80)

		return errorCount == 0
