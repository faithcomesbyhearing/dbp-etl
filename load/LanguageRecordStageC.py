from Config import *
from LanguageReader import LanguageRecordInterface
from SQLUtility import *
from StageCLanguageService import getMediaByIdAndFormat 

class LanguageRecordStageC (LanguageRecordInterface):
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
        return 'Live' if self.record.get(LanguageRecordStageC.propertiesName['status']) == 'Complete' else self.record.get(LanguageRecordStageC.propertiesName['status'])

    def Id(self):
        return self.record.get(LanguageRecordStageC.propertiesName['id'])

    def Reg_StockNumber(self):
        return self.record.get(LanguageRecordStageC.propertiesName['regStockNumber'])

    def derivativeOf(self):
        return self.record.get(LanguageRecordStageC.propertiesName['derivativeOf'])

    def LangName(self):
        return self.record.get(LanguageRecordStageC.propertiesName['langName'])

    def ISO(self):
        return self.record.get(LanguageRecordStageC.propertiesName['iso'])

    def Country(self):
        return self.record.get(LanguageRecordStageC.propertiesName['country'])

    def DBP_Equivalent(self):
        return self.record.get(LanguageRecordStageC.propertiesName['dbpEquivalent'])

    def Volumne_Name(self):
        return self.record.get(LanguageRecordStageC.propertiesName['volumneName'])

    def Orthography(self, index):
        return self.record.get(LanguageRecordStageC.propertiesName['orthography'])

    def Licensor(self):
        pass

    def CoLicensor(self):
        pass

    def Copyrightc(self):
        return None
