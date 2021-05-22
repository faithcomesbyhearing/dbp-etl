# UnicodeScript.py


import io
import math
import unicodedata
from Config import *
from LPTSExtractReader import *

class UnicodeScript:


	def readFile(self, filePath):
		text = []
		fp = io.open(filePath, mode="r", encoding="utf-8")
		for i in range(0, 10):
			discard = fp.readline()
			#print("discard", discard)
		for line in fp:
			#print("read", line)
			pos = 0
			while ">" in line[pos:]:
				pos = line.index(">", pos) + 1
				if len(line) > (pos + 20):
					#print("pos", pos)
					c = line[pos]
					while c != "<":
						if c.isalpha():
							text.append(c)
						pos += 1
						if pos >= len(line):
							c = "<"
						else:
							c = line[pos]
					#print("now", "".join(text))
				if len(text) > 1000:
					fp.close
					#print("Found", "".join(text))
					return text
		fp.close
		return text
				
			
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
		for (script, count) in scriptSet.items():
			#print("iterate scripts", script, count)
			if count > mostCount:
				mostCount = count
				mostScript = script
		return mostScript


	def matchScripts(self, fileScript, lptsScript):
		if fileScript == None:
			return False
		if lptsScript != None:
			lptsScript = lptsScript.upper()
			lptsScript = lptsScript.split(" ")[0]
		if fileScript == lptsScript:
			return True
		return False


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
	setTypeCode = 'text_plain'
	sql = ("SELECT distinct c.bible_id, f.id, t.description AS stock_no, f.hash_id"
		" FROM bible_filesets f, bible_fileset_connections c, bible_fileset_tags t"
		" WHERE c.hash_id = f.hash_id AND c.hash_id = t.hash_id"
		" AND f.set_type_code = %s AND t.name = 'stock_no' ORDER BY c.bible_id, f.id")

	dbpFilesetList = db.select(sql, (setTypeCode,))
	print("DBP filesets to process", len(dbpFilesetList))
	unicodeScript = UnicodeScript()
	for (bibleId, filesetId, stockNo, hashId) in dbpFilesetList:
		print(bibleId, filesetId, stockNo)
		lptsRecord = lptsReader.getByStockNumber(stockNo)
		textDamIds = lptsRecord.DamIdList("text")
		textDamIds = lptsRecord.ReduceTextList(textDamIds)

		#bibleId = 'ARBBIB'  
		#filesetId = 'ARBBIB'
		#stockNo = 'N1ARB/BIB'
		#textDamIds = [('ARBBIBN_ET', 1, 'Live')]
		trySet = set()
		for (damId, index, status) in textDamIds:
			if not index in trySet:
				trySet.add(index)
				print(" ", damId, index, status)
		
				if setTypeCode == "text_format":
					objKey = "text/%s/%s" % (bibleId, filesetId[:6])
					print("prefix", objKey)
					fileKeys = []
					request = { 'Bucket': 'dbp-prod', 'MaxKeys':10, 'Prefix': objKey }
					response = s3Client.list_objects_v2(**request)
					for item in response.get('Contents', []):
						filename = item.get('Key')
						if filename.endswith(".html") and not filename.endswith("about.html") and not filename.endswith("index.html"):
							fileKeys.append(filename)
					if len(fileKeys) == 0:
						print(objKey, "has no files?")
					if bibleId == "AMPWBT":
						print("files", fileKeys)

					pos = 0
					text = []
					while len(text) < 200 and len(fileKeys) > pos:
						s3Client.download_file("dbp-prod", fileKeys[pos], "./sample.html")
						text += unicodeScript.readFile("./sample.html")
						pos += 1

				elif setTypeCode == "text_plain":
					dbpText = db.selectList("SELECT verse_text FROM bible_verses WHERE hash_id = %s limit 10", (hashId,))
					#print(dbpText)
					text = []
					for dbpVerse in dbpText:
						for char in dbpVerse:
							text.append(char)

				print("text", "".join(text[:60]))

				fileScript = unicodeScript.findScript(text)
				print("fileScript", fileScript)
				lptsScript = lptsRecord.Orthography(index)

				isMatch = unicodeScript.matchScripts(fileScript, lptsScript)
				if isMatch:
					print("******* MATCH", bibleId, filesetId, fileScript, index)
				else:
					print("******* No match for", bibleId, filesetId, fileScript, index, lptsScript)


				#sys.exit()

