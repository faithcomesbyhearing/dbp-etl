class PreValidateResult:

	def __init__(self, languageRecord, filesetId, damId, typeCode, index, fileList = None):
		self.languageRecord = languageRecord
		self.filesetId = filesetId
		self.damId = damId
		self.typeCode = typeCode
		self.index = index
		self.fileList = fileList

	def bibleId(self):
		return self.languageRecord.DBP_EquivalentByIndex(self.index)

	def scope(self):
		return self.damId[6]

	def script(self):
		return self.languageRecord.Orthography(self.index)

	def toString(self):
		results = []
		results.append("out: %s/%s/%s is %s %d" % (self.typeCode, self.bibleId(), self.filesetId, self.damId, self.index))
		if self.fileList != None:
			for file in self.fileList:
				results.append(file)
		return ", ".join(results)		
