

import os

def outputDir(dirname, outfile):
	files = os.listdir(dirname)
	for file in files:
		if file.endswith("csv"):
			fp = open(dirname + os.sep + file, "r")
			for line in fp:  # skip first line fp[1:]?
				parts = line.split(",")
				lineout = "/".join(parts[1:5])
				outfile.write("%s\n" % (lineout))
			fp.close()


outfile = open("test1.out", "w")
outputDir("../../files/validate/accepted/", outfile)
outputDir("../../files/validate/duplicate/", outfile)
outputDir("../../files/validate/quarantine/", outfile)
outfile.close()


