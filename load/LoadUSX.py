

import re
from LPTSExtractReader import *


config = Config()
lpts = LPTSExtractReader(config)
stockNoMap = lpts.getStockNumberMap()

BUCKET = "USX-Test"


with open("/Volumes/FCBH/TextUSXDirs/usx.txt", "r") as usx:
	for row in usx:
		row = row.strip()
		found = False
		parts = re.split(r'[ _/]', row)
		for part in parts:
			#print(part)
			stockNo = part[:-3] + "/" + part[-3:]
			#print(stockNo)
			if stockNo in stockNoMap.keys():
				#print("StockNo", stockNo)
				#print("Directory", row)
				found = True
				#numDamIds = 0
				damId6 = set()
				record = stockNoMap[stockNo]
				for index in [1,2,3]:
					damIds = record.TextDamIdMap(index)
					for (damId, status) in damIds.items():
						#print("DamId", damId, status)
						damId6.add(damId[:6])
					#numDamIds += len(damIds)
				#print("DamId6", damId6)
				cmd = 'aws s3 sync "%s" s3://%s/%s/%s' % (row, BUCKET, stockNo, "_".join(damId6))
				print(cmd)
				#print("")
				break
		#if not found:
			#print("NOT FOUND", row)

"""
		#print(row.strip())
		dirs = row.split("/")
		#print(dirs[0])
		stockLang = dirs[0].split(" ")[-1]
		#print(stockLang)
		if stockLang[0] == "[" or stockLang[0] == "{":
			lang = stockLang[1:-1]
			#print(lang, dirs[1])
		else:
			stockNo = stockLang[:-3] + "/" + stockLang[-3:]
			#print(stockLang, stockNo)
			# lookup stock no in LPTS
			# see if there is only 1 text DamId
			#print(stockNo, "|", dirs[1:])
			if stockNo not in stockNoMap.keys():
				print("********", stockNo, dirs[1:])
"""