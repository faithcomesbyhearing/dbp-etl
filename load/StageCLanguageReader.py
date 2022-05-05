from LanguageReader import LanguageReaderInterface
from StageCLanguageService import StageCLanguageService as LanguageService
from StageCLanguageServiceParse import parseResult;

class StageCLanguageReader (LanguageReaderInterface):
    def __init__(self):
        self.service = LanguageService()

    def getBibleIdMap(self):
        result = parseResult(self.service.getAllMediaRecords())

        listRecords = {}

        for rowTuple in result:
            if listRecords.get(rowTuple.DBP_Equivalent()) == None:
                newList = []
                newList.append(rowTuple)
                listRecords[rowTuple.DBP_Equivalent()] = newList
            else:
                listRecords[rowTuple.DBP_Equivalent()].append(rowTuple)
        return listRecords

    def getLanguageRecord(self, typeCode, bibleId, filesetId):
       raise Exception("Not implemented")

    def getByStockNumber(self, stockNumber):
        result = parseResult(self.service.getMediaByStocknumber(stockNumber))
        return result[0] if len(result) > 0 else None

    def getFilesetRecords(filesetId):
        raise Exception("Not implemented")

    def getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId):
       raise Exception("Not implemented")

    def reduceCopyrightToName(lptsCopyright):
        raise Exception("Not implemented")

    def getFilesetRecords10(self, filesetId):
        mediaId = self.service.getMediaIdFromFilesetId(filesetId)

        if mediaId != None:
            result = parseResult(self.service.getMediaById(mediaId))
            languageRecord = result[0] if len(result) > 0 else None
            response = set()
            response.add((languageRecord, languageRecord.Status(), ''))
            return response

        return None

    def getFilesetRecords(self, filesetId):
        raise Exception("Not implemented")