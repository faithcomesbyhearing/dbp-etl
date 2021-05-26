# UnicodeScript.py


import io
import math
import unicodedata
from Config import *
from LPTSExtractReader import *

class UnicodeScript:


	## Returns a list of files in a bucket of on a local disk.
	def getFilenames(s3Client, location, filesetPath):
		results = []
		ignoreSet = {"Thumbs.db"}
		if not location.startswith("s3://"):
			pathname = location + os.sep + filesetPath
			if os.path.isdir(pathname):
				for filename in [f for f in os.listdir(pathname) if not f.startswith('.')]:
					if filename not in ignoreSet and os.path.isfile(pathname + os.sep + filename):
						filepath = pathname + os.sep + filename
						results.append(filepath)
			else:
				print("ERROR: Invalid pathname %s" % (pathname,))
		else:
			bucket = location[5:]
			print("bucket", bucket)
			request = { 'Bucket': bucket, 'MaxKeys': 1000, 'Prefix': filesetPath + "/" }
			response = s3Client.list_objects_v2(**request)
			for item in response.get('Contents', []):
				objKey = item.get('Key')
				results.append(objKey)
			if len(results) == 0:
				print("ERROR: Invalid bucket %s or prefix %s/" % (bucket, filesetPath))
		return results


	## Downloads an objects, returns content as an array of lines, but discards first 10 lines
	def readS3Object(s3Client, s3Bucket, s3Key):
		response = s3Client.get_object(Bucket=s3Bucket, Key=s3Key)
		content = response['Body'].read().decode("utf-8")
		lines = content.split("\n") if content != None else []
		lines = lines[10:] # discard first 10 lines
		#print("read", lines)
		return lines


	## Opens a file and returns as an array of strings, but discards first 10 lines
	def readFile(filePath):
		file = open(filePath, mode='r', encoding="utf-8")
		lines = file.readlines()
		file.close()
		lines = lines[10:] # discard first 10 lines
		#print("***** read", len(lines))
		print(lines)
		return lines


	## Parses XML contents and returns an array of characters
	def parseXMLStrings(lines):
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
	def textToArray(contents):
		text = []
		for line in contents:
			for char in line:
				if char.isalpha():
					text.append(char)
		return text
				
		
	## Returns the script code of text based upon results returned by unicodedata	
	def findScript(text):
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
	def matchScripts(fileScript, lptsScript):
		if fileScript == None:
			return False
		if lptsScript != None:
			lptsScript = lptsScript.upper()
			lptsScript = lptsScript.split(" ")[0]
		if fileScript == lptsScript:
			return True
		return False



	## This is a convenience method used to check a script using verse_text
	def checkScripts(db, filesetId, lptsScript):
		sampleText = UnicodeScript.findVerseText(db, filesetId[:6]) ## can't be in this class
		textList = UnicodeScript.textToArray(sampleText)
		actualScript = UnicodeScript.findScript(textList)
		isMatch = UnicodeScript.matchScripts(actualScript, lptsScript)
		return isMatch


if (__name__ == '__main__'):
	import time
	import boto3
	import csv
	from AWSSession import *
	from SQLUtility import *

	config = Config.shared()
	s3Client = AWSSession.shared().s3Client
	lptsReader = LPTSExtractReader(config.filename_lpts_xml)
	db = SQLUtility(config)
	#setTypeCode = 'text_plain'
	setTypeCode = 'text_format'
	sql = ("SELECT distinct c.bible_id, f.id, t.description AS stock_no, f.hash_id"
		" FROM bible_filesets f, bible_fileset_connections c, bible_fileset_tags t"
		" WHERE c.hash_id = f.hash_id AND c.hash_id = t.hash_id"
		" AND f.set_type_code = %s AND t.name = 'stock_no' ORDER BY c.bible_id, f.id")
	dbpFilesetList = db.select(sql, (setTypeCode,))
	print("DBP filesets to process", len(dbpFilesetList))

	with open("UnicodeScript.csv", 'w', newline='\n') as csvfile:
		writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(("OK/NOT", "stockNo", "bibleId", "filesetId6", "index", "sample text", "actual script", "pct match", "lpts script", "message"))

		for (bibleId, filesetId, stockNo, hashId) in dbpFilesetList:
			print(bibleId, filesetId, stockNo)
			lptsRecord = lptsReader.getByStockNumber(stockNo)
			textDamIds = lptsRecord.DamIdList("text")
			textDamIds = lptsRecord.ReduceTextList(textDamIds)

			trySet = set()
			for (damId, index, status) in textDamIds:
				if not index in trySet:
					trySet.add(index)
					print(" ", damId, index, status)
					message = None
					lptsScript = lptsRecord.Orthography(index)
					if lptsScript == None:
						message = "No LPTS Script"

					if setTypeCode == "text_format":
						objKey = "text/%s/%s" % (bibleId, filesetId[:6])
						fileKeys = UnicodeScript.getFilenames(s3Client, "s3://dbp-prod", objKey)
						#print("fileKeys", fileKeys)
						if len(fileKeys) == 0:
							message = "DBP has no files"
						
						pos = 1 # skip first file
						lines = []
						while len("".join(lines)) < 2000 and len(fileKeys) > pos:
							filename = fileKeys[pos]
							if filename.endswith(".html") and not filename.endswith("about.html") and not filename.endswith("index.html"):
								lines += UnicodeScript.readS3Object(s3Client, "dbp-prod", filename)
							pos += 1
						textList = UnicodeScript.parseXMLStrings(lines)

					elif setTypeCode == "text_plain":
						#sql = "SELECT verse_text FROM bible_verses WHERE hash_id IN (SELECT hash_id FROM bible_filesets WHERE id = %s) limit 10"
						#sampleText = db.selectList(sql, (filesetId,))
						sampleText = db.selectList("SELECT verse_text FROM bible_verses WHERE hash_id = %s limit 10", (hashId,))

						if len(sampleText) == 0 and message == None:
							message = "No verse text"
							textList = []
						else:
							textList = UnicodeScript.textToArray(sampleText)

					print("text", "".join(textList[:60]))

					(fileScript, pctMatch) = UnicodeScript.findScript(textList)
					print("fileScript", fileScript, pctMatch)
					lptsScript = lptsRecord.Orthography(index)
					if lptsScript == None:
						message = "No LPTS Script"

					isMatch = UnicodeScript.matchScripts(fileScript, lptsScript)
					if isMatch:
						print("******* MATCH", bibleId, filesetId, fileScript, index)
						matchAns = "OK"
					else:
						print("******* No match for", bibleId, filesetId, fileScript, index, lptsScript)
						matchAns = "NOT"
						if message == None:
							message = "Mismatch"

					writer.writerow((matchAns, stockNo, bibleId, damId, index, "".join(textList[:20]), fileScript, str(pctMatch) + "%", lptsScript, message))


