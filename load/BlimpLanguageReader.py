from LanguageReader import LanguageReaderInterface
from BlimpLanguageService import BlimpLanguageService as LanguageService
from BlimpLanguageServiceParse import parseResult

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
        result = parseResult(self.service.getMediaByStocknumber(stockNumber))
        return result[0] if len(result) > 0 else None

    def getLanguageRecordLoose(self, typeCode, bibleId, dbpFilesetId):
        damId = dbpFilesetId[:10]

        if len(damId) == 10 and damId[-2:] == "SA":
            damId = damId[:8] + "DA"

        (mediaId, modeType) = self.service.getMediaIdFromFilesetId(damId)

        if modeType != typeCode:
            return (None, None)

        result = parseResult(self.service.getMediaById(mediaId))
        languageRecord = result[0] if len(result) > 0 else None

        return (languageRecord, None)

    def reduceCopyrightToName(lptsCopyright):
        raise Exception("Not implemented")

    def getFilesetRecords10(self, filesetId):
        damId = filesetId[:10]

        if len(damId) == 10 and damId[-2:] == "SA":
            damId = damId[:8] + "DA"

        (mediaId, modeType) = self.service.getMediaIdFromFilesetId(damId)
        if mediaId == None:
            filesetIdWithoutMusic = damId[:7] + "_" + damId[8:10]
            (mediaId, modeType) = self.service.getMediaIdFromFilesetId(filesetIdWithoutMusic)

        if mediaId != None:
            result = parseResult(self.service.getMediaById(mediaId))
            languageRecord = result[0] if len(result) > 0 else None
            response = set()
            modeTypeCamelCase = modeType
            if modeType != None:
                modeTypeCamelCase = modeType.capitalize()

            response.add((languageRecord, languageRecord.Status(), modeTypeCamelCase))
            return response

        return None

    def getFilesetRecords(self, filesetId):
        (mediaId, _) = self.service.getMediaIdFromFilesetId(filesetId[:10])

        if mediaId != None:
            result = parseResult(self.service.getMediaById(mediaId))
            languageRecord = result[0] if len(result) > 0 else None
            response = set()
            response.add((languageRecord.Status(), languageRecord))
            return response

        return None

    def getStocknumber(self, stocknumber):
        (record) = self.service.getStocknumber(stocknumber)
        return record
