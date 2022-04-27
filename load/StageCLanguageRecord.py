from LanguageReader import LanguageRecordInterface
from StageCLanguageService import getMediaByIdAndFormat 

class StageCLanguageRecord (LanguageRecordInterface):
    propertiesName = {
        "id": "Id",
        "regStockNumber": "Reg_StockNumber",
        "extent": "extent",
        "country": "Country",
        "iso": "ISO",
        "langName": "LangName",
        "dbpEquivalent": "DBP_Equivalent",
        "licensor": "Licensor",
        "copyrightc": "Copyrightc",
        "copyrightp": "Copyrightp",
        "volumneName": "Volumne_Name",
        "dbpMobile": "DBPMobile",
        "dbpWebHub": "DBPWebHub",
        "mobileText": "MobileText",
        "hubText": "HubText",
        "apiDevText": "APIDevText",
        "apiDevAudio": "APIDevAudio",
        "ethName": "EthName",
        "altName": "AltName",
        "webHubVideo": "WebHubVideo",
        "copyrightVideo": "Copyright_Video",
        "apiDevVideo": "APIDevVideo",
        "status": "status",
        "derivativeOf": "derivativeOf",
        "orthography": "Orthography",
    }

    def __init__(self, record):
        self.record = record
    
    def DamIdList(self, typeCode):
        return getMediaByIdAndFormat(self.derivativeOf(), typeCode)
    
    def ReduceTextList(self, damIdList):
        damIdSet = set()
        for (damId, index, status) in damIdList:
            damIdOut = damId
            damIdSet.add((damIdOut, index, status))
        return damIdSet

    def DamIdMap(self, typeCode, index):
        raise Exception("Not implemented")

    def Status(self):
        return 'Live' if self.record.get(StageCLanguageRecord.propertiesName['status']) == 'Complete' else self.record.get(StageCLanguageRecord.propertiesName['status'])

    def Id(self):
        return self.record.get(StageCLanguageRecord.propertiesName['id'])

    def Reg_StockNumber(self):
        return self.record.get(StageCLanguageRecord.propertiesName['regStockNumber'])

    def derivativeOf(self):
        return self.record.get(StageCLanguageRecord.propertiesName['derivativeOf'])

    def LangName(self):
        return self.record.get(StageCLanguageRecord.propertiesName['langName'])

    def ISO(self):
        return self.record.get(StageCLanguageRecord.propertiesName['iso'])

    def Country(self):
        return self.record.get(StageCLanguageRecord.propertiesName['country'])

    def DBP_Equivalent(self):
        return self.record.get(StageCLanguageRecord.propertiesName['dbpEquivalent'])

    def Volumne_Name(self):
        return self.record.get(StageCLanguageRecord.propertiesName['volumneName'])

    def Orthography(self, index):
        return self.record.get(StageCLanguageRecord.propertiesName['orthography'])

    def EthName(self):
        return self.record.get(StageCLanguageRecord.propertiesName['ethName'])

    def AltName(self):
        return self.record.get(StageCLanguageRecord.propertiesName['altName'])

    def Licensor(self):
        pass

    def CoLicensor(self):
        pass

    def Copyrightc(self):
        return None
