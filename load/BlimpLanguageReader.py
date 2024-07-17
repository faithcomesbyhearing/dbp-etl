from LanguageReader import LanguageReaderInterface

# FIXME(2101) - this is copied from StageCLanguageReader, but not implemented. 
# instead of reading from xml file, query the database. 
# Since the BlimpLanguageReader should only be called during content load, only implement 
# the methods needed during content loading
class BlimpLanguageReader (LanguageReaderInterface):
    def __init__(self):
        self.service = LanguageService()

    @property
    def resultSet(self):
        raise Exception("Not implemented")

    def getBibleIdMap(self):
        raise Exception("Not implemented")

    def getLanguageRecord(self, typeCode, bibleId, filesetId):
       raise Exception("Not implemented")

    def getByStockNumber(self, stockNumber):
        raise Exception("Not implemented")

    def getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId):
       raise Exception("Not implemented")

    def reduceCopyrightToName(lptsCopyright):
        raise Exception("Not implemented")

    def getFilesetRecords10(self, filesetId):
        raise Exception("Not implemented")

    def getFilesetRecords(self, filesetId):
        raise Exception("Not implemented")
