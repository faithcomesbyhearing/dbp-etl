# FindDuplicateFilesets.py
#
# This validation program search the generated CSV files for duplicate filesets
# And decides which one to keep
#

# 1. Go through all of the accept or accept + quarantine
# 2. Build a Map of filesetid
# 3. Find duplicates
# 4. get the first, or one filename from each.
# 5. Do a select on dbp from the fileset, and first file
# 6. Generate a report showing each of the duplicates and 

import os
import csv
from Config import *
from Log import *

### This class is not used as of 6/21/21. It was intended to be used for processing the dbp-prod, dbp-vid
### buckets, but the dbp-etl program has not been used in that way.

class FindDuplicateFilesets:

	def __init__(self, config):
		self.acceptedDir = config.directory_accepted
		self.duplicateDir = config.directory_duplicate


	def findDuplicates(self):
		filesets = {}
		files = os.listdir(self.acceptedDir)
		for file in sorted(files):
			if file.endswith("csv"):
				file = file.split(".")[0]
				parts = file.split("_")
				filesetId = parts[2]
				filenames = filesets.get(filesetId, [])
				filenames.append(file)
				filesets[filesetId] = filenames
		result = {}
		for filesetId, filenames in filesets.items():
			resultFilenames = []
			if len(filenames) > 1:
				for filename in filenames:
					(actualFilename, datetime) = self.findOneFile(self.acceptedDir, filename + ".csv")
					resultFilenames.append((filename, datetime))
				result[filesetId] = resultFilenames
		return result


	def findOneFile(self, dirname, filename):
		with open(dirname + os.sep + filename, newline='\n') as csvfile:
			reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			firstLine = True
			for row in reader:
				if firstLine:
					firstLine = False
				else:
					return (row[3], row[9])


	def moveDuplicates(self, duplicatesMap):
		for filesetId, resultFilenames in duplicatesMap.items():
			indexOfAccepted = self.findMostRecent(resultFilenames)
			acceptedPrefix = resultFilenames[indexOfAccepted][0].replace("_", "/")
			for index in range(len(resultFilenames)):
				if index != indexOfAccepted:
					#print("Move %s %sfile to duplicate" % (filesetId, resultFilenames[index]))
					logger = Log.getLogger(filesetId)
					logger.message(Log.INFO, "Entire Fileset moved to duplicate, %s accepted." % (acceptedPrefix,))
					filename = resultFilenames[index][0] + ".csv"
					source = self.acceptedDir + os.sep + filename
					target = self.duplicateDir + os.sep + filename
					os.rename(source, target)


	def findMostRecent(self, filenames):
		# These are exceptions that are to be selected even though they
		# are not the most current
		acceptedHacks = {"audio_ENGESV_ENGESVN1DA16",
					"audio_KMYWBT_KMAWBTN2DA",
					"audio_PORNLE_PO1NLHC1DA",
					"audio_PORNLE_PO1NLHN1DA",
					"audio_PORNLE_PO1NLHO1DA"}
		datetimes = []
		for index in range(len(filenames)):
			#print("filename to compare", filenames[index][0])
			if filenames[index][0] in acceptedHacks:
				return index
			datetimes.append(filenames[index][1])
		for (filename, datetime) in filenames:
			datetimes.append(datetime)
		mostRecent = max(datetimes)
		return datetimes.index(mostRecent)


	## debug only
	def report(self, duplicates):
		for filesetId, filenames in duplicates.items():
			print(filesetId)
			for (filename, datetime) in filenames:
				print(" ", filename, datetime)
			print(" ")


#config = Config("dev")
#find = FindDuplicateFilesets(config)
#duplicates = find.findDuplicates()
#find.moveDuplicates(duplicates)
#find.report(duplicates)

"""
ENGESVN1DA16
  dbp-prod/audio/EN1ESV/ENGESVN1DA16/B01___01_Matthew_____ENGESVN1DA.mp3 2019-09-24 20:22:55+00:00  ** this one is latest
  dbp-prod/audio/ENGESV/ENGESVN1DA16/B01___01_Matthew_____ENGESVN1DA.mp3 2019-06-19 19:27:08+00:00 ** this one is in DBP
  DBP:dbp-prod/ENGESV/ENGESVN1DA16/B07___01_1CorinthiansENGESVN1DA.mp3

KMAWBTN2DA
  dbp-prod/audio/KMAWBT/KMAWBTN2DA/B01___01_Matthew_______N2KMAWBT.mp3 2019-02-26 22:57:28+00:00  ** this one is latest
  dbp-prod/audio/KMYWBT/KMAWBTN2DA/B01___01_Matthew_____KMAWBTN2DA.mp3 2018-08-08 01:03:32+00:00  ** this one is in DBP
  DBP:dbp-prod/KMYWBT/KMAWBTN2DA/B07___01_1CorinthiansKMAWBTN2DA.mp3

PO1NLHC1DA
  dbp-prod/audio/PORNLE/PO1NLHC1DA/A01___01_Genesis_____PO1NLHO1DA.mp3 2019-07-31 13:27:32+00:00 ** this one is in DBP
 dbp-prod/audio/PORNLH/PO1NLHC1DA/A01___01_Genesis_____PO1NLHO1DA.mp3 2019-08-01 19:29:57+00:00 ** this one is latest
  DBP:dbp-prod/PORNLE/PO1NLHC1DA/A13___01_1Cronicas___PO1NLHO1DA.mp3

PO1NLHN1DA
  dbp-prod/audio/PORNLE/PO1NLHN1DA/B01___01_S_Mateus____PO1NLHN1DA.mp3 2019-07-31 13:48:00+00:00 ** this one is in DBP
  dbp-prod/audio/PORNLH/PO1NLHN1DA/B01___01_S_Mateus____PO1NLHN1DA.mp3 2019-08-01 19:29:33+00:00 ** this one is latest
  DBP:dbp-prod/PORNLE/PO1NLHN1DA/B07___01_1Corintios__PO1NLHN1DA.mp3

PO1NLHO1DA
  dbp-prod/audio/PORNLE/PO1NLHO1DA/A01___01_Genesis_____PO1NLHO1DA.mp3 2019-07-31 13:49:18+00:00 ** this one is in DBP
  dbp-prod/audio/PORNLH/PO1NLHO1DA/A01___01_Genesis_____PO1NLHO1DA.mp3 2019-08-01 19:28:11+00:00 ** this one is latest
  DBP:dbp-prod/PORNLE/PO1NLHO1DA/A13___01_1Cronicas___PO1NLHO1DA.mp3
"""




