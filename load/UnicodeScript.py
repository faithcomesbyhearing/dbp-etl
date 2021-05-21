# UnicodeScript.py


import io
import math
import unicodedata
from Config import *
from LPTSExtractReader import *

class UnicodeScript:

	def findScriptName(self, filePath):
		text = self.readFile(filePath)
		#print("final", text)
		script = self.findScript(text)
		return(script)


	def readFile(self, filePath):
		text = []
		fp = io.open(filePath, mode="r", encoding="utf-8")
		for i in range(0, 10):
			discard = fp.readline()
			#print("discard", discard)
		for line in fp:
			#print("read", line)
			if ">" in line:
				pos = line.index(">") + 1
				if len(line) > (pos + 100):
					#print("pos", pos)
					c = line[pos]
					while c != "<":
						if c.isalpha():
							text.append(c)
						pos += 1
						c = line[pos]
						#print(text)
				if len(text) > 100:
					fp.close
					return text
		fp.close
		return text
				
			
	def findScript(self, text):
		scriptSet = {}
		for c in text:
			if unicodedata.category(c) in {"Lu", "Ll"}:
				name = unicodedata.name(c)
				#print("name", name)
				scriptName = name.split(" ")[0]
				count = scriptSet.get(scriptName, 0)
				count += 1
				scriptSet[scriptName] = count
		mostCount = 0
		mostScript = None
		for (script, count) in scriptSet.items():
			#print("iterate scripts", script, count)
			if count > mostCount:
				mostCount = count
				mostScript = script
		return mostScript


	def getLPTSScripts(self, filesetId):
		#print("search", filesetId)
		results = []
		lptsRecords = lptsReader.getFilesetRecords10(filesetId)
		if lptsRecords != None:
		#	print("record for %s is not found" % (filesetId))
		#	sys.exit()
		#else:
			for (lptsRecord, status, fieldName) in lptsRecords:
				#print("found1", status, fieldName)
				if "3" in fieldName:
					bibleId = lptsRecord.DBP_Equivalent3()
					scriptName = lptsRecord.Orthography(3)
				elif "2" in fieldName:
					bibleId = lptsRecord.DBP_Equivalent2()
					scriptName = lptsRecord.Orthography(2)
				else:
					bibleId = lptsRecord.DBP_Equivalent()
					scriptName = lptsRecord.Orthography(1)
				if scriptName != None:
					scriptName = scriptName.upper()
				#print("found2", scriptName, bibleId)
				results.append((scriptName, bibleId))
		return results


	def matchScripts(self, fileScript, lptsScripts):
		#print("match fileScript", fileScript)
		for (scriptName, bibleId) in lptsScripts:
			#print("match2", fileScript, scriptName)
			if fileScript == scriptName:
				return bibleId
		return None


#MAIN
# write a method that skips through dbp-prod, reading lines.
# it stops at every 100th line
# it downloads the file

#METHOD
# it opens the file and reads the first 100 characters
# for every Lu Ll character it puts it in a map with count
# we get the character name with the largest count
# we compare that to each script code, in lpts for the 

"""
u = chr(233) + chr(0x0bf2) + chr(3972) + chr(6000) + chr(13231)

for i, c in enumerate(u):
    print(i, '%04x' % ord(c), unicodedata.category(c), end=" ")
    print(unicodedata.name(c))

# Get numeric value of second character
print(unicodedata.numeric(u[1]))

"""

#u = chr(233) + chr(0x0bf2) + chr(3972) + chr(6000) + chr(13231)
"""
nameSet = set()
for i in range(41, 1114112):
	c = chr(i)
	
	try:
		category = unicodedata.category(c)
		if category == 'Lu' or category == 'Ll':
			print(c, hex(ord(c)), unicodedata.category(c))
			name = unicodedata.name(c)
			scripts = name.split(" ")
			script = scripts[0] + " " + scripts[1] if scripts[0] == "OLD" else scripts[0]
			print(name)
			nameSet.add(script)
	except:
		print("no name")

for name in nameSet:
	print(name)
"""
#for i, c in enumerate(u):
#    print(i, '%04x' % ord(c), unicodedata.category(c), end=" ")
#    print(unicodedata.name(c))

# Get numeric value of second character
#print(unicodedata.numeric(u[1]))


if (__name__ == '__main__'):
	import boto3
	from AWSSession import *

	config = Config.shared()
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	unicodeScript = UnicodeScript()
	count = 0
	dbpProd = io.open(config.directory_bucket_list + "dbp-prod.txt", mode="r", encoding="utf-8")
	for line in dbpProd:
		objKey = line.split("\t")[0]
		count += 1
		if math.remainder(count, 100) == 0:
			if objKey.startswith("text") and objKey.endswith(".html"):
				print(count, objKey)

				parts = objKey.split("/")
				filesetId = parts[2]
				allLptsScripts = []
				allLptsScripts += unicodeScript.getLPTSScripts(filesetId + "N1ET")
				allLptsScripts += unicodeScript.getLPTSScripts(filesetId + "N2ET")
				allLptsScripts += unicodeScript.getLPTSScripts(filesetId + "O1ET")
				allLptsScripts += unicodeScript.getLPTSScripts(filesetId + "O2ET")
				#print("allScript", allLptsScripts)

				if len(allLptsScripts) > 0:			
					AWSSession.shared().s3Client.download_file("dbp-prod", objKey, "./sample.html")
					fileScript = unicodeScript.findScriptName("./sample.html")

					#print("final", fileScript)
					#sys.exit()


					bibleId = unicodeScript.matchScripts(fileScript, allLptsScripts)
					if bibleId == None:
						print("******* No match for", filesetId, fileScript, allLptsScripts)
					else:
						print("******* MATCH", bibleId, filesetId, fileScript)






