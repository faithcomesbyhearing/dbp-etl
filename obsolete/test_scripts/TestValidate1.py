

import os
import csv

def outputDir(dirname):
	files = os.listdir(dirname)
	for file in files:
		if file.endswith("csv"):
			with open(dirname + os.sep + file, newline='\n') as csvfile:
				reader = csv.reader(csvfile, delimiter=',', quotechar='"')
				firstLine = True
				for row in reader:
					if firstLine:
						firstLine = False
					else:
						print("/".join(row[0:4]))


def outputFile(filename):
	with open(filename, newline='\n') as csvfile:
		reader = csv.reader(csvfile, delimiter=',', quotechar='"')
		firstLine = True
		for row in reader:
			if firstLine:
				firstLine = False
			else:
				num = len(row) - 1
				print("/".join(row[0:num]))


outputDir("../../files/validate/accepted/")
outputDir("../../files/validate/duplicate/")
outputDir("../../files/validate/quarantine/")
outputFile("../../files/validate/errors/IgnoredFiles.csv")
