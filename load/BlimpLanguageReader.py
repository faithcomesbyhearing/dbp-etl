from LanguageReader import LanguageReaderInterface
from BlimpLanguageService import BlimpLanguageService as LanguageService
from typing import List
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
            if languageRecord == None:
                return None
            response = set()
            response.add((languageRecord.Status(), languageRecord))
            return response

        return None

    def getStocknumber(self, stocknumber):
        (record) = self.service.getStocknumber(stocknumber)
        return record
    # Get the list of books for the Gospels and Apostolic History group
    def getGospelsAndApostolicHistoryBooks(self):
       return self.service.getGospelsAndApostolicHistoryMap()
    # Get the list of books for the Covenant group
    def getCovenantBookId(self):
        return "COV"

    def get_existing_files_by_fileset_id(self, fileset_id: str) -> List[str]:
        """
        Fetch all file names that already exist for this fileset in the database.
        Truncates fileset_id[:10] (per legacy behavior) and returns an empty list if there is no data.
        """
        # Legacy: service expects only the first 10 chars
        lookup_id = fileset_id[:10]
        records = self.service.get_files_by_fileset_id(lookup_id) or []
        # Extract file names from the records
        return [file_name for _, file_name in records]

    def list_existing_and_loaded_files(self, input_fileset) -> List[str]:
        """
        Combine the set of audio files in the InputFileset with the files
        already in the DB for that fileset, returning a deduped list.
        """
        # audioFileNames() might return None or [], so default to empty list
        loaded = set(input_fileset.audioFileNames() or [])
        existing = set(self.get_existing_files_by_fileset_id(input_fileset.filesetId))
        # Union gives us de-duplicated filenames
        combined = loaded | existing
        return sorted(combined)
