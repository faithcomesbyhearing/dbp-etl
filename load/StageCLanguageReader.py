from LanguageReader import LanguageReaderInterface


class StageCLanguageReader (LanguageReaderInterface):
    def getBibleIdMap(self):
       raise Exception("Not implemented")

    def getByStockNumber(self, stockNumber):
       raise Exception("Not implemented")
    
    def getLPTSRecord(self, typeCode, bibleId, filesetId):
       raise Exception("Not implemented")

    def getFilesetRecords(filesetId):
       raise Exception("Not implemented")

    def getLPTSRecordLoose(typeCode, bibleId, dbpFilesetId):
       raise Exception("Not implemented")

    def reduceCopyrightToName(lptsCopyright):
       raise Exception("Not implemented")

    def getFilesetRecords10(damId):
       raise Exception("Not implemented")
