# LPTSExtractReader
#
# This program reads the LPTS Extract and produces hash tables.
#

import io
import sys
import os
from xml.dom import minidom
# from LanguageReader import LanguageReaderInterface, LanguageRecordInterface
from LanguageReader import LanguageReaderInterface
from LanguageRecord import LanguageRecord


class LPTSExtractReader (LanguageReaderInterface):

	def __init__(self, lptsExtractPath):
		self.lptsExtractPath = lptsExtractPath
		self.resultSet = []
		dom = None
		try:
			doc = minidom.parse(lptsExtractPath)
		except Exception as err:
			print("FATAL ERROR parsing LPTS extract", err)
			sys.exit(1)
		root = doc.childNodes
		if len(root) != 1 or root[0].nodeName != "dataroot":
			print ("ERROR: First node of LPTS Export must be <docroot>")
			sys.exit()
		else:
			for recNode in root[0].childNodes:
				if recNode.nodeType == 1:
					if recNode.nodeName != "qry_dbp4_Regular_and_NonDrama":
						print("ERROR: Child nodes in LPTS Export must be 'qry_dbp4_Regular_and_NonDrama'")
						sys.exit()
					else:
						resultRow = {}

						for fldNode in recNode.childNodes:
							if fldNode.nodeType == 1:
								# print(fldNode.nodeName + " = " + fldNode.firstChild.nodeValue)
								resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue
						#print("\n\n\n *** creating an languageRecord object ***\n\n\n")
						self.resultSet.append(LanguageRecord(resultRow))
		self.checkRecordCount()
		self.bibleIdMap = self.getBibleIdMap()
		self.filesetIdMap = None
		self.filesetIdMap10 = None
		self.stockNumMap = None
		print("LPTS Extract with %d records and %d BibleIds is loaded." % (len(self.resultSet), len(self.bibleIdMap.keys())))


	# Check the record count against prior run to make certain we have the entire file. And save new record count.
	# Check the record count against prior run to make certain we have the entire file. And save new record count.
	def checkRecordCount(self):
		try:
			filename = "/tmp/dbp-etl-LPTS_count.txt"
			nowCount = len(self.resultSet)
			if os.path.isfile(filename):
				with open(filename, "r") as cntIn:
					priorCount = cntIn.read().strip()
					if priorCount.isdigit():
						if (int(priorCount) - nowCount) > 50:
							print("FATAL: %s is too small. Prior run was %s records. Now it has %d records. This count is stored at %s" 
								% (self.lptsExtractPath, priorCount, nowCount, filename))
							sys.exit()
						else:
							print("LPTS Size. Prior: %s, Current: %s" % (priorCount, nowCount))
			else:
				print("first run of LPTS Reader -- did not find %s" % (filename))
				
			with open(filename, "w") as cntOut:
				cntOut.write(str(nowCount))
				#print("wrote count %s to file %s" % (nowCount, filename))
		except FileNotFoundError:
			print("Exception: first run of LPTS Reader -- did not find %s" % (filename))

    ## Generates Map bibleId: [(index, languageRecord)], called by class init
	def getBibleIdMap(self):
		bibleIdMap = {}
		for rec in self.resultSet:
			bibleId = rec.DBP_Equivalent()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((1, rec))
				bibleIdMap[bibleId] = records
			bibleId = rec.DBP_Equivalent2()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((2, rec))
				bibleIdMap[bibleId] = records
			bibleId = rec.DBP_Equivalent3()
			if bibleId != None:
				records = bibleIdMap.get(bibleId, [])
				records.append((3, rec))
				bibleIdMap[bibleId] = records
		return bibleIdMap


	## Returns the LPTS Record for a stockNumber
	def getByStockNumber(self, stockNumber):
		if self.stockNumMap == None:
			self.stockNumMap = {}
			for rec in self.resultSet:
				stockNum = rec.Reg_StockNumber()
				if stockNum != None:
					if self.stockNumMap.get(stockNum) == None:
						self.stockNumMap[stockNum] = rec
					else:
						print("ERROR: Duplicate Stock Num", stockNum);
				else:
					print("INFO: the following record has no stock number: EthName: %s and AltName: %s" % (rec.EthName(), rec.AltName()))
		return self.stockNumMap.get(stockNumber)


	## Returns one (record, index) for typeCode, bibleId, filesetId
	## This is a strict method that only returns when BibleId matches and status is Live
	def getLanguageRecord(self, typeCode, bibleId, filesetId):
		normFilesetId = filesetId[:10]
		languageRecords = self.bibleIdMap.get(bibleId)
		if languageRecords != None:
			for (index, record) in languageRecords:
				damIdSet = record.DamIds(typeCode, index)
				if normFilesetId in damIdSet:
					return (record, index)
		return (None, None)


	## This method is a hack, because in LPTS a text damId is 10 char, 
	## but in DBP it is 6 char, when searching LPTS the same text damid
	## can be found in multiple record, but there is no way to know
	## which is correct.
	def getLanguageRecordLoose(self, typeCode, bibleId, filesetId):
		result = self.getLanguageRecord(typeCode, bibleId, filesetId)
		if result[0] != None:
			return result
		result = self.getFilesetRecords(filesetId)
		if result == None or len(result) == 0:
			return (None, None)
		for (status, record) in result:
			if status == "Live":
				return (record, None)
		first = result[0]
		record = first[1]
		return (record, None)


	## This method returns all LPTS records regardless of the status
	## Otherwise it is exactly like getLanguageRecord
	## Return [(record, index)]
	def getLanguageRecordsAll(self, typeCode, bibleId, filesetId):
		results = []
		normFilesetId = filesetId[:10]
		languageRecords = self.bibleIdMap.get(bibleId)
		if languageRecords != None:
			for (index, record) in languageRecords:
				damIdList = record.DamIdMap(typeCode, index)
				if normFilesetId in damIdList:
					#return(record, index)
					results.append((record, index))
		#return (None, None)
		return results		


	## This is a more permissive way to get LPTS Records, it does not require
	## a type or bibleId.  So, it can only be used for non-index fields
	## It returns an array of statuses and records, i.e. [(status, languageRecord)]
	def getFilesetRecords(self, filesetId):
		if self.filesetIdMap == None:
			self.filesetIdMap = {}


			for languageRecord in self.resultSet:
				record = languageRecord.record
				damIdDict = dict(
					list(LanguageRecord.audio1DamIdDict.items()) + 
					list(LanguageRecord.audio2DamIdDict.items()) + 
					list(LanguageRecord.audio3DamIdDict.items()) +
					list(LanguageRecord.text1DamIdDict.items()) + 
					list(LanguageRecord.text2DamIdDict.items()) + 
					list(LanguageRecord.text3DamIdDict.items()) +
					list(LanguageRecord.videoDamIdDict.items()) 
					)
				hasKeys = set(damIdDict.keys()).intersection(set(record.keys()))
				for key in hasKeys:
					statusKey = damIdDict[key]
					damId = record[key]
					if "Text" in key:
						damId = damId[:6]
					status = record.get(statusKey)
					statuses = self.filesetIdMap.get(damId, [])
					statuses.append((status, languageRecord))
					self.filesetIdMap[damId] = statuses
					if "Text" in key: # Put in second key for Text filesets with underscore
						damId = record[key]
						damId = damId[:7] + "_" + damId[8:]
						statuses = self.filesetIdMap.get(damId, [])
						statuses.append((status, languageRecord))
						self.filesetIdMap[damId] = statuses
		return self.filesetIdMap.get(filesetId[:10], None)


	## This method is different than getFilesetRecords in that it expects an entire 10 digit text filesetId
	## It also removes excess characters for filesets and corrects for SA types.
	## This method does not return the 1, 2, 3 index
	## It returns an array of statuses and records, i.e. Set((languageRecord, status, fieldName))
	def getFilesetRecords10(self, filesetId):	
		if self.filesetIdMap10 == None:
			self.filesetIdMap10 = {}
			damIdDict = dict(
					list(LanguageRecord.audio1DamIdDict.items()) + 
					list(LanguageRecord.audio2DamIdDict.items()) + 
					list(LanguageRecord.audio3DamIdDict.items()) + 
					list(LanguageRecord.text1DamIdDict.items()) + 
					list(LanguageRecord.text2DamIdDict.items()) +
					list(LanguageRecord.text3DamIdDict.items()) +
					list(LanguageRecord.videoDamIdDict.items())
					)
			for languageRecord in self.resultSet:
				record = languageRecord.record
				hasKeys = set(damIdDict.keys()).intersection(set(record.keys()))
				for key in hasKeys:
					statusKey = damIdDict[key]
					damId = record[key]
					status = record.get(statusKey)
					statuses = self.filesetIdMap10.get(damId, set())
					statuses.add((languageRecord, status, key))
					self.filesetIdMap10[damId] = statuses
		damId = filesetId[:10]
		if len(damId) == 10 and damId[-2:] == "SA":
			damId = damId[:8] + "DA";	
		return self.filesetIdMap10.get(damId, None)


	## This method returns a list of field names for development the purposes.
	def getFieldNames(self):
		nameCount = {}
		for rec in self.resultSet:
			for field in rec.record.keys():
				count = nameCount.get(field, 0)
				count += 1
				nameCount[field] = count
		for field in sorted(nameCount.keys()):
			count = nameCount[field]
			print(field, count)


	## This method is used to reduce copyright messages to a shorter string that
	## will be matched with copyrights in the table lpts_copyright_organizations
	## in order to determine the organization_id for the copyright holder.
	def reduceCopyrightToName(self, name):
		pos = 0
		if "©" in name:
			pos = name.index("©")
		elif "℗" in name:
			pos = name.index("℗")
		elif "®" in name:
			pos = name.index("®")
		if pos > 0:
			name = name[pos:]
		for char in ["©", "℗", "®","0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ",", "/", "-", ";", "_", ".", "\t", "\n"]:
			name = name.replace(char, "")
		name = name.replace("     ", " ")
		name = name.replace("    ", " ")
		name = name.replace("   ", " ")
		name = name.replace("  ", " ")
		name = name[:64]
		return name.strip()

"""
*********************************************************
*** Everything below here is intended for unit test only.
*********************************************************
"""

"""
# Get listing of damIds, per stock no
if __name__ == '__main__':
	config = Config.shared()
	reader = LanguageReaderCreator().create(config)
	for languageRecord in reader.resultSet:
		stockNo = languageRecord.Reg_StockNumber()
		for typeCode in ["audio", "text", "video"]:
			mediaDamIds = []
			indexes = [1] if typeCode == "video" else [1,2,3]
			for index in indexes:
				damIds = languageRecord.DamIdMap(typeCode, index)
				if len(damIds) > 0:
					print(stockNo, typeCode, index, damIds.keys())
"""

"""
# Get alphabetic list distinct field names
if (__name__ == '__main__'):
	fieldCount = {}
	config = Config()
	reader = LPTSExtractReader(config)
	for languageRecord in reader.resultSet:
		record = languageRecord.record
		for fieldName in record.keys():
			#print(fieldName)
			count = fieldCount.get(fieldName, 0)
			count += 1
			fieldCount[fieldName] = count
	for fieldName in sorted(fieldCount.keys()):
		count = fieldCount[fieldName]
		print("%s,%s" % (fieldName, count))
"""

"""
if (__name__ == '__main__'):
	config = Config()
	reader = LPTSExtractReader(config)
	reader.getFieldNames()
"""
"""
if (__name__ == '__main__'):
	config = Config()
	reader = LPTSExtractReader(config)
	for rec in reader.resultSet:
		for index in [1, 2, 3]:
			print(rec.Reg_StockNumber(), index)
			#prefixSet = set()
			for typeCode in ["text", "audio", "video"]:
				if typeCode != "video" or index == 1:
					damidSet = rec.DamIds2(typeCode, index)
					if len(damidSet) > 0:
						print(typeCode, index, damidSet)
						#for damid in damidSet:
						#	prefixSet.add(damid[:6])
			#if len(prefixSet) > 1:
			#	print("ERROR: More than one DamId prefix in set %s" % (",".join(prefixSet)))
"""
"""
# Find bible_ids that exist in multiple stock numbers
# Find stock numbers that contain multiple bible_ids
if (__name__ == '__main__'):
	bibleIdMap = {}
	config = Config()
	reader = LPTSExtractReader(config)
	for rec in reader.resultSet:
		#print(rec.Reg_StockNumber(), rec.DBP_Equivalent(), rec.DBP_Equivalent2())
		#if len(rec.Reg_StockNumber()) != 9:
		#	print("Different Stocknumber", rec.Reg_StockNumber())
		stockNo = rec.Reg_StockNumber()
		bibleId1 = rec.DBP_Equivalent()
		bibleId2 = rec.DBP_Equivalent2()
		#if bibleId2 != None:
		#	print("StockNo:", stockNo, "BibleId 1:", bibleId1, "BibleId 2:", bibleId2)
		#if bibleId1 in bibleIdMap.keys():
		#	priorStockNum = bibleIdMap[bibleId1]
		#	print("StockNo:", stockNo, "StockNo:", priorStockNum, "BibleId1:", bibleId1)
		if bibleId2 in bibleIdMap.keys():
			priorStockNum = bibleIdMap[bibleId2]
			print("StockNo:", stockNo, "StockNo:", priorStockNum, "BibleId2:", bibleId2)
		if bibleId1 != None:
			bibleIdMap[bibleId1] = stockNo
		if bibleId2 != None:
			bibleIdMap[bibleId2] = stockNo
"""


if (__name__ == '__main__'):
	from Config import *
	from LanguageReaderCreator import *	
	result = {}
	config = Config()
	reader = LanguageReaderCreator("B").create(config.filename_lpts_xml)
	for rec in reader.resultSet:
		textDamIds = rec.DamIdList("text")
		textDamIds = rec.ReduceTextList(textDamIds)
		#if len(textDamIds) > 1:
		#	print(textDamIds)
		listDamIds = list(textDamIds)
		if len(listDamIds) > 1 and listDamIds[0][0][:6] != listDamIds[1][0][:6]:
			print(rec.Reg_StockNumber(), listDamIds)


