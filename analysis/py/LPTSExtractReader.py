# LPTSExtractReader
#
# This program reads the LPTS Extract and produces hash tables.
#

import io
import sys
import os
from xml.dom import minidom
from Config import *

class LPTSExtractReader:

	def __init__(self, config):
		self.resultSet = []
		doc = minidom.parse(config.directory_lpts_xml)
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
								#print(fldNode.nodeName + " = " + fldNode.firstChild.nodeValue)
								resultRow[fldNode.nodeName] = fldNode.firstChild.nodeValue

						self.resultSet.append(resultRow)


	def getStockNoMap(self):
		stockNoMap = {}
		for row in self.resultSet:
			stockNum = row[u'Reg_StockNumber']
			#print(stockNum)
			stockNoMap[stockNum] = row
		return stockNoMap


	def getBibleIdMap(self):
		bibleIdMap = {}
		for row in self.resultSet:
			bibleId = row.get(u'DBP_Equivalent')
			if bibleId != None and bibleId != "N/A" and bibleId != "#N/A":
				#print(bibleId)
				bibleIdMap[bibleId] = row
			bibleId2 = row.get(u'DBP_Equivalent2')
			if bibleId2 != None:
				#print bibleId2
				bibleIdMap[bibleId2] = row
		return bibleIdMap


	def getAudioMap(self):
		audioDic = {
			"CAudioDAMID1": "CAudioStatus1",
			"CAudioDamStockNo": "CAudioDamStatus",
			"ND_NTAudioDamID1": "ND_NTAudioDamIDStatus1",
			"ND_NTAudioDamID2": "ND_NTAudioDamIDStatus2",
			"ND_OTAudioDamID1": "ND_OTAudioDamIDStatus1",
			"ND_OTAudioDamID2": "ND_OTAudioDamIDStatus2",
			"Reg_NTAudioDamID1": "Reg_NTAudioDamIDStatus1",
			"Reg_NTAudioDamID2": "Reg_NTAudioDamIDStatus2",
			"Reg_OTAudioDamID1": "Reg_OTAudioDamIDStatus1",
			"Reg_OTAudioDamID2": "Reg_OTAudioDamIDStatus2"}
		return self.isInMap(audioDic)


	def getTextMap(self):
		textDic = {
			"ND_NTTextDamID1": "ND_NTTextDamIDStatus1",
			"ND_NTTextDamID2": "ND_NTTextDamIDStatus2", 
			"ND_NTTextDamID3": "ND_NTTextDamIDStatus3",
			"ND_OTTextDamID1": "ND_OTTextDamIDStatus1",
			"ND_OTTextDamID2": "ND_OTTextDamIDStatus2", 
			"ND_OTTextDamID3": "ND_OTTextDamIDStatus3",
			"Reg_NTTextDamID1": "Reg_NTTextDamIDStatus1",
			"Reg_NTTextDamID2": "Reg_NTTextDamIDStatus2",
			"Reg_NTTextDamID3": "Reg_NTTextDamIDStatus3", 
			"Reg_OTTextDamID1": "Reg_OTTextDamIDStatus1", 
			"Reg_OTTextDamID2": "Reg_OTTextDamIDStatus2",  
			"Reg_OTTextDamID3": "Reg_OTTextDamIDStatus3"}
		resultMap = {}
		for row in self.resultSet:
			for damIdKey in textDic.keys():
				if damIdKey in row:
					damId = row[damIdKey]
					statusName = textDic[damIdKey]
					status = row.get(statusName)
					if status == "Live":
						#print(damIdKey, damId, statusName, status)
						resultMap[damId[:6]] = row
		return resultMap	
	

	def getVideoMap(self):
		videoDic = {
			"Video_John_DamStockNo": "Video_John_DamStatus",
			"Video_Luke_DamStockNo": "Video_Luke_DamStatus",
			"Video_Mark_DamStockNo": "Video_Mark_DamStatus",
			"Video_Matt_DamStockNo": "Video_Matt_DamStatus"}
		return self.isInMap(videoDic)


	def isInMap(self, damIdMap):
		resultMap = {}
		for row in self.resultSet:
			for damIdKey in damIdMap.keys():
				if damIdKey in row:
					damId = row[damIdKey]
					statusName = damIdMap[damIdKey]
					status = row.get(statusName)
					if status == "Live":
						#print(damIdKey, damId, statusName, status)
						resultMap[damId] = row
		return resultMap	




"""
config = Config()
reader = LPTSExtractReader(config)
#for item in reader.resultSet:
#	print(item)
#stMap = reader.getStockNoMap()
#bibleMap = reader.getBibleIdMap()
#for item in bibleMap.keys():
#	print(item, bibleMap[item])
#audioMap = reader.getAudioMap()
#textMap = reader.getTextMap()
videoMap = reader.getVideoMap()
"""






