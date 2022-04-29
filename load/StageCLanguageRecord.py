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
        "mobileVideo": "MobileVideo",
        "copyrightVideo": "Copyright_Video",
        "apiDevVideo": "APIDevVideo",
        "status": "status",
        "derivativeOf": "derivativeOf",
        "orthography": "Orthography",
    }

    def __init__(self, record):
        self.record = {
            self.propertiesName['apiDevAudio']: "-1",
            self.propertiesName['dbpMobile']: "-1",
            self.propertiesName['dbpWebHub']: "-1",
            self.propertiesName['apiDevText']: "-1",
            self.propertiesName['mobileText']: "-1",
            self.propertiesName['hubText']: "-1",
            self.propertiesName['apiDevVideo']: "-1",
            self.propertiesName['mobileVideo']: "-1",
            self.propertiesName['webHubVideo']: "-1"
        }

        self.record.update(record)
    
    def DamIdList(self, typeCode):
        return getMediaByIdAndFormat(self.derivativeOf(), typeCode)
    
    def ReduceTextList(self, damIdList):
        damIdSet = set()
        for (damId, index, status) in damIdList:
            damIdOut = damId
            damIdSet.add((damIdOut, index, status))
        return damIdSet

    def DamIdMap(self, typeCode, index):
        return {
            self.Id(): self.Status()
        }

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

    def DBP_EquivalentByIndex(self, index):
        if index == 1:
            return self.DBP_Equivalent()
        else:
            print("ERROR: DBP_Equivalent index must be 1, 2, or 3.")
            sys.exit()

    def Volumne_Name(self):
        return self.record.get(StageCLanguageRecord.propertiesName['volumneName'])

    def Orthography(self, index):
        return self.record.get(StageCLanguageRecord.propertiesName['orthography'])

    def EthName(self):
        return self.record.get(StageCLanguageRecord.propertiesName['ethName'])

    def AltName(self):
        return self.record.get(StageCLanguageRecord.propertiesName['altName'])

    def APIDevAudio(self):
        return self.record.get(StageCLanguageRecord.propertiesName['apiDevAudio'])

    def DBPMobile(self):
        return self.record.get(StageCLanguageRecord.propertiesName['dbpMobile'])

    def DBPWebHub(self):
        return self.record.get(StageCLanguageRecord.propertiesName['dbpWebHub'])

    def APIDevText(self):
        return self.record.get(StageCLanguageRecord.propertiesName['apiDevText'])

    def MobileText(self):
        return self.record.get(StageCLanguageRecord.propertiesName['mobileText'])

    def HubText(self):
        return self.record.get(StageCLanguageRecord.propertiesName['hubText'])

    def APIDevVideo(self):
        return self.record.get(StageCLanguageRecord.propertiesName['apiDevVideo'])

    def MobileVideo(self):
        return self.record.get(StageCLanguageRecord.propertiesName['mobileVideo'])

    def WebHubVideo(self):
        return self.record.get(StageCLanguageRecord.propertiesName['webHubVideo'])

    def Licensor(self):
        pass

    def CoLicensor(self):
        pass

    def Copyrightc(self):
        return None
