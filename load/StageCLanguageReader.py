from LanguageReader import LanguageReaderInterface


class StageCLanguageReader (LanguageReaderInterface):
    def getBibleIdMap(self):
        print("TBD. getBibleIdMap... ")
        pass 

    def getByStockNumber(self, stockNumber):
        print("TBD. getByStockNumber... ")
        pass
    
    def getLPTSRecord(self, typeCode, bibleId, filesetId):
        pass

    def getFilesetRecords(filesetId):
        pass 

    def getLPTSRecordLoose(typeCode, bibleId, dbpFilesetId):
        pass 

    def reduceCopyrightToName(lptsCopyright):
        pass

    def getFilesetRecords10(damId):
        pass
