from LanguageReader import LanguageReaderInterface
from StageCLanguageService import StageCLanguageService as LanguageService
from StageCLanguageServiceParse import parseResult

class StageCLanguageReader (LanguageReaderInterface):
    def __init__(self):
        self.service = LanguageService()

    @property
    def resultSet(self):
        try:
            return self.value
        except AttributeError:
            self.value = parseResult(self.service.getAllMediaRecords())
            return self.value

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

    def getLanguageRecordLoose(typeCode, bibleId, dbpFilesetId):
       raise Exception("Not implemented")

    def reduceCopyrightToName(lptsCopyright):
        raise Exception("Not implemented")

    def getFilesetRecords10(self, filesetId):
        damId = filesetId[:10]

        if len(damId) == 10 and damId[-2:] == "SA":
            damId = damId[:8] + "DA"

        mediaId = self.service.getMediaIdFromFilesetId(filesetId)

        if mediaId != None:
            result = parseResult(self.service.getMediaById(mediaId))
            languageRecord = result[0] if len(result) > 0 else None
            response = set()
            response.add((languageRecord, languageRecord.Status(), ''))
            return response

        return None

    def getFilesetRecords(self, filesetId):
        mediaId = self.service.getMediaIdFromFilesetId(filesetId[:10])

        if mediaId != None:
            result = parseResult(self.service.getMediaById(mediaId))
            languageRecord = result[0] if len(result) > 0 else None
            response = set()
            response.add((languageRecord.Status(), languageRecord))
            return response

        return None
