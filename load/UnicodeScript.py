# UnicodeScript.py


import io
import math
import unicodedata
from LPTSExtractReader import *

class UnicodeScript:


	def __init__(self):
		self.errors = []


	## Returns a list of files in a bucket of on a local disk.
	def getFilenames(self, s3Client, location, filesetPath):
		results = []
		ignoreSet = {"Thumbs.db"}
		if not location.startswith("s3://"):
			pathname = location + os.sep + filesetPath
			if os.path.isdir(pathname):
				for filename in [f for f in os.listdir(pathname) if not f.startswith('.')]:
					if filename not in ignoreSet:# and os.path.isfile(pathname + os.sep + filename):
						results.append(filename)
			else:
				self.errors.append("ERROR: Invalid pathname %s" % (pathname,))
		else:
			bucket = location[5:]
			#print("bucket", bucket)
			request = { 'Bucket': bucket, 'MaxKeys': 1000, 'Prefix': filesetPath + "/" }
			response = s3Client.list_objects_v2(**request)
			for item in response.get('Contents', []):
				objKey = item.get('Key')
				filename = objKey[len(filesetPath) + 1:]
				results.append(filename)
			if len(results) == 0:
				self.errors.append("ERROR: Invalid bucket %s or prefix %s/" % (bucket, filesetPath))
		return results


	## Downloads an objects, returns content as an array of lines, but discards first 10 lines
	def readObject(self, s3Client, location, filePath):
		print("UnicodeScript:readObject. location: %s, filepath: %s" % (location, filePath))
		if location.startswith("s3://"):
			s3Bucket = location[5:]
			response = s3Client.get_object(Bucket=s3Bucket, Key=filePath)
			content = response['Body'].read().decode("utf-8")
			lines = content.split("\n") if content != None else []
			lines = lines[10:] # discard first 10 lines
		else:
			file = open(location +'/'+filePath, mode='r', encoding="utf-8")
			lines = file.readlines()
			file.close()
			lines = lines[10:] # discard first 10 lines
		#print("read", lines)
		return lines	


	## Parses XML contents and returns an array of characters
	def parseXMLStrings(self, lines):
		text = []
		inText = False
		for line in lines:
			for char in line:
				if char == "<":
					inText = False
				if inText and char.isalpha():
					text.append(char)
				if char == ">":
					inText = True
		return text


	## Converts an array of text string to a array of text chars, which is needed for findScript
	def textToArray(self, contents):
		text = []
		for line in contents:
			for char in line:
				if char.isalpha():
					text.append(char)
		return text
				
		
	## Returns the script code of text based upon results returned by unicodedata	
	def findScript(self, text):
		scriptSet = {}
		for c in text:
			#print(c, unicodedata.category(c))
			if unicodedata.category(c) in {"Lu", "Ll", "Lo"}:
				name = unicodedata.name(c)
				#print("name", name)
				scriptName = name.split(" ")[0]
				count = scriptSet.get(scriptName, 0)
				count += 1
				scriptSet[scriptName] = count
		#print(scriptSet)
		mostCount = 0
		mostScript = None
		message = []
		totalCount = 0
		for (script, count) in scriptSet.items():
			message.append("%s=%d" % (script, count))
			totalCount += count
			#print("iterate scripts", script, count)
			if count > mostCount:
				mostCount = count
				mostScript = script
		pctMatch = int(scriptSet.get(mostScript) / totalCount * 100.0) if mostScript != None else 0
		if mostScript == "CJK":
			mostScript = "HAN"
		if mostScript == "MYANMAR":
			mostScript = "BURMESE"
		return (mostScript, pctMatch)


	## Compares the actual script determined from text, and the lpts Script
	def matchScripts(self, fileScript, lptsScript):
		if fileScript == None:
			return False
		if lptsScript != None:
			lptsScript = lptsScript.upper()
			lptsScript = lptsScript.split(" ")[0]
		if fileScript == lptsScript:
			return True
		return False


	## Used from inside Validate to that existing damId's in stockNo have the correct script
	def validateStockNoRecord(self, lptsRecord, db):
		stockNo = lptsRecord.Reg_StockNumber()
		damIdList = lptsRecord.DamIdList("text")
		#print(damIdList)
		damIdList = lptsRecord.ReduceTextList(damIdList)
		for (damId, index, status) in damIdList:
			lptsScript = lptsRecord.Orthography(index)
			sql = "SELECT verse_text FROM bible_verses WHERE hash_id IN (SELECT hash_id FROM bible_filesets WHERE id = %s) limit 10"
			sampleText = db.selectList(sql, (damId[:6],))
			if sampleText != None and len(sampleText) > 0:
				textList = self.textToArray(sampleText)
				#print("text", "".join(textList[:60]))
				(fileScript, pctMatch) = self.findScript(textList)
				#print("fileScript", fileScript, pctMatch)
				isMatch = self.matchScripts(fileScript, lptsScript)
				if not isMatch:
					fileScript = fileScript.capitalize() if fileScript != None else None
					#print("******* No match for", damId, fileScript, index, lptsScript)
					self.errors.append("Script code incorrect in %s for damId %s; Change %s to %s" % (stockNo, damId, lptsScript, fileScript))
		return self.errors


if (__name__ == '__main__'):
	import time
	import boto3
	import csv
	from Config import *	
	from AWSSession import *
	from SQLUtility import *

	config = Config.shared()
	db = SQLUtility(config)
	s3Client = AWSSession.shared().s3Client
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	unicodeScript = UnicodeScript()
	setTypeCode = 'text_plain'
	#setTypeCode = 'text_format'

	with open("UnicodeScript.csv", 'w', newline='\n') as csvfile:
		writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(("OK/NOT", "stockNo", "bibleId", "filesetId6", "index", "sample text", "actual script", "pct match", "lpts script", "message"))

		for lptsRecord in lptsReader.resultSet:
			stockNo = lptsRecord.Reg_StockNumber()
			damIdList = lptsRecord.DamIdList("text")
			#print(damIdList)
			damIdList = lptsRecord.ReduceTextList(damIdList)
			for (damId, index, status) in damIdList:
				if status in { "Live", "live" }:
					bibleId = lptsRecord.DBP_EquivalentByIndex(index)
					lptsScript = lptsRecord.Orthography(index)
					print(stockNo, damId, index, bibleId)
					message = None
					if lptsScript == None:
						message = "No LPTS Script"

					if setTypeCode == "text_format":
						objKey = "text/%s/%s" % (bibleId, filesetId[:6])
						fileKeys = unicodeScript.getFilenames(s3Client, "s3://dbp-prod", objKey)
						#print("fileKeys", fileKeys)
						if len(fileKeys) == 0:
							message = "DBP has no files"
						
						pos = 1 # skip first file
						lines = []
						while len("".join(lines)) < 2000 and len(fileKeys) > pos:
							filename = fileKeys[pos]
							if filename.endswith(".html") and not filename.endswith("about.html") and not filename.endswith("index.html"):
								lines += unicodeScript.readObject(s3Client, "s3://dbp-prod", objKey + "/" + filename)
							pos += 1
						textList = unicodeScript.parseXMLStrings(lines)

					elif setTypeCode == "text_plain":
						sql = "SELECT verse_text FROM bible_verses WHERE hash_id IN (SELECT hash_id FROM bible_filesets WHERE id = %s) limit 10"
						sampleText = db.selectList(sql, (damId[:6],))

						if len(sampleText) == 0 and message == None:
							message = "No verse text"
							textList = []
						else:
							textList = unicodeScript.textToArray(sampleText)

					print("text", "".join(textList[:60]))

					(fileScript, pctMatch) = unicodeScript.findScript(textList)
					print("fileScript", fileScript, pctMatch)
					lptsScript = lptsRecord.Orthography(index)
					if lptsScript == None:
						message = "No LPTS Script"

					isMatch = unicodeScript.matchScripts(fileScript, lptsScript)
					if isMatch:
						print("******* MATCH", bibleId, damId, fileScript, index)
						matchAns = "OK"
					else:
						print("******* No match for", bibleId, damId, fileScript, index, lptsScript)
						matchAns = "NOT"
						if message == None:
							message = "Mismatch"

					writer.writerow((matchAns, stockNo, bibleId, damId, index, "".join(textList[:20]), fileScript, str(pctMatch) + "%", lptsScript, message))

