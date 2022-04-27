import unittest
import os
from LPTSExtractReader import LPTSExtractReader
from StageCLanguageReader import StageCLanguageReader
from Config import *

class TestLanguageReaderStage(unittest.TestCase):
    def setUp(self):
        config = Config()
        self.LPTSReader = LPTSExtractReader(config.filename_lpts_xml)
        self.languageReader = StageCLanguageReader()

    def assert_by_specific_media(self, stageCResult, stageBResult):
        self.assertEqual(stageCResult.Reg_StockNumber(), stageBResult.Reg_StockNumber(), "Reg_StockNumber for N1SPAPDT should be [%s]" % stageBResult.Reg_StockNumber())
        self.assertEqual(stageCResult.ISO(), stageBResult.ISO(), "ISO for N1SPAPDT should be [%s]" % stageBResult.ISO())
        self.assertEqual(stageCResult.DBP_Equivalent(), stageBResult.DBP_Equivalent(), "DBP_Equivalent for N1SPAPDT should be [%s]" % stageBResult.DBP_Equivalent())
        self.assertEqual(stageCResult.Volumne_Name(), stageBResult.Volumne_Name(), "Volumne_Name for N1SPAPDT should be [%s]" % stageBResult.Volumne_Name())
        self.assertEqual(stageCResult.EthName(), stageBResult.EthName(), "EthName for N1SPAPDT should be [%s]" % stageBResult.EthName())
        
        ## For now, the next tests will be commented until the missing data in the agreements databaseìssue is solved

        # self.assertEqual(stageCResult.LangName(), stageBResult.LangName(), "LangName for N1SPAPDT should be [%s]" % stageBResult.LangName())
        # self.assertEqual(stageCResult.Country(), stageBResult.Country(), "Country for N1SPAPDT should be [%s]" % stageBResult.Country())
        # self.assertEqual(stageCResult.DBPMobile(), stageBResult.DBPMobile(), "DBPMobile for N1SPAPDT should be [%s]" % stageBResult.DBPMobile())
        # self.assertEqual(stageCResult.DBPWebHub(), stageBResult.DBPWebHub(), "DBPWebHub for N1SPAPDT should be [%s]" % stageBResult.DBPWebHub())
        # self.assertEqual(stageCResult.MobileText(), stageBResult.MobileText(), "MobileText for N1SPAPDT should be [%s]" % stageBResult.MobileText())
        # self.assertEqual(stageCResult.HubText(), stageBResult.HubText(), "HubText for N1SPAPDT should be [%s]" % stageBResult.HubText())
        # self.assertEqual(stageCResult.APIDevText(), stageBResult.APIDevText(), "APIDevText for N1SPAPDT should be [%s]" % stageBResult.APIDevText())
        # self.assertEqual(stageCResult.APIDevAudio(), stageBResult.APIDevAudio(), "APIDevAudio for N1SPAPDT should be [%s]" % stageBResult.APIDevAudio())
        # self.assertEqual(stageCResult.AltName(), stageBResult.AltName(), "AltName for N1SPAPDT should be [%s]" % stageBResult.AltName())
        # self.assertEqual(stageCResult.WebHubVideo(), stageBResult.WebHubVideo(), "WebHubVideo for N1SPAPDT should be [%s]" % stageBResult.WebHubVideo())
        # self.assertEqual(stageCResult.Copyright_Video(), stageBResult.Copyright_Video(), "Copyright_Video for N1SPAPDT should be [%s]" % stageBResult.Copyright_Video())
        # self.assertEqual(stageCResult.APIDevVideo(), stageBResult.APIDevVideo(), "APIDevVideo for N1SPAPDT should be [%s]" % stageBResult.APIDevVideo())

    # Test the getBibleIdMap method using length of bible list
    # @unittest.skip("skipping Total BIBLES using getBibleIdMap")
    def test_get_bible_id_map_total_media(self):
        listStageC = self.languageReader.getBibleIdMap()
        listStageB = self.LPTSReader.getBibleIdMap()

        self.assertEqual(len(listStageC), len(listStageB), "Total BIBLES: stage B [%s] and stage C [%s] " % (len(listStageB), len(listStageC)))

    # Test the getBibleIdMap method using a specific bible
    def test_get_bible_id_map_by_specific_bible(self):
        listStageC = self.languageReader.getBibleIdMap()
        listStageB = self.LPTSReader.getBibleIdMap()

        self.assertEqual(len(listStageC.get('SPAPDT')), len(listStageB.get('SPAPDT')), "Total media records for the Bible ID SPAPDT Name [La Palabra de Dios para Todos]: stage B [%s] and stage C [%s]" % (len(listStageB.get('SPAPDT')), len(listStageC.get('SPAPDT'))))

    def test_get_bible_id_map_by_specific_english_standard_version_bible(self):
        listStageC = self.languageReader.getBibleIdMap()
        listStageB = self.LPTSReader.getBibleIdMap()

        self.assertEqual(len(listStageC.get('EN1ESV')), len(listStageB.get('EN1ESV')), "Total media records for the Bible ID EN1ESV Name [English Standard Version]: stage B [%s] and stage C [%s]" % (len(listStageB.get('EN1ESV')), len(listStageC.get('EN1ESV'))))

    # Test the getByStockNumber method using a specific stocknumber
    def test_get_by_stocknumber_by_specific_media(self):
        stockNumber = 'N1SPAPDT'
        stageBNumWithFormat = self.LPTSReader.getStocknumberWithFormat(stockNumber)
        stageBResult = self.LPTSReader.getByStockNumber(stageBNumWithFormat)
        stageCNumWithFormat = self.languageReader.getStocknumberWithFormat(stockNumber)
        stageCResult = self.languageReader.getByStockNumber(stageCNumWithFormat)

        self.assert_by_specific_media(stageCResult, stageBResult)

    # Test the getByStockNumber method using a specific stocknumber with audio format
    def test_get_by_stocknumber_by_specific_media_audio(self):
        stockNumber = 'N2ACDWBT'
        stageBNumWithFormat = self.LPTSReader.getStocknumberWithFormat(stockNumber)
        stageBResult = self.LPTSReader.getByStockNumber(stageBNumWithFormat)
        stageCNumWithFormat = self.languageReader.getStocknumberWithFormat(stockNumber)
        stageCResult = self.languageReader.getByStockNumber(stageCNumWithFormat)

        self.assert_by_specific_media(stageCResult, stageBResult)

    # Test the getFilesetRecords10 method using a damId(media Id) value
    def test_get_fileset_records_10(self):
        damId = 'GNDWYIN2DA'
        stageBResult = self.LPTSReader.getFilesetRecords10(damId)
        stageCResult = self.languageReader.getFilesetRecords10(damId)

        languageRecordB = None
        languageRecordC = None

        for (languageRecord, status, key) in stageBResult:
            languageRecordB = languageRecord
        for (languageRecord, status, key) in stageCResult:
            languageRecordC = languageRecord
        
        self.assert_by_specific_media(languageRecordC, languageRecordB)

    def printResultStageC(self, stageCResult):
        print("")
        print("========================== RESULT STAGE C ========================================")
        print(stageCResult)
        print("Reg_StockNumber: %s" % stageCResult.Reg_StockNumber())
        print("LangName: %s" % stageCResult.LangName())
        print("ISO: %s" % stageCResult.ISO())
        print("Country: %s" % stageCResult.Country())
        print("DBP_Equivalent: %s" % stageCResult.DBP_Equivalent())
        print("Volumne_Name: %s" % stageCResult.Volumne_Name())

        print("==================================================================================")
        print("")

    def printResultStageB(self, stageBResult):
        print("")
        print("========================== RESULT STAGE B ========================================")
        print(stageBResult)
        print("Reg_StockNumber: %s" % stageBResult.Reg_StockNumber())
        print("LangName: %s" % stageBResult.LangName())
        print("ISO: %s" % stageBResult.ISO())
        print("Country: %s" % stageBResult.Country())
        print("DBP_Equivalent: %s" % stageBResult.DBP_Equivalent())
        print("Licensor: %s" % stageBResult.Licensor())
        print("Copyrightc: %s" % stageBResult.Copyrightc())
        print("Copyrightp: %s" % stageBResult.Copyrightp())
        print("Volumne_Name: %s" % stageBResult.Volumne_Name())
        print("DBPMobile: %s" % stageBResult.DBPMobile())
        print("DBPWebHub: %s" % stageBResult.DBPWebHub())
        print("MobileText: %s" % stageBResult.MobileText())
        print("HubText: %s" % stageBResult.HubText())
        print("APIDevText: %s" % stageBResult.APIDevText())
        print("APIDevAudio: %s" % stageBResult.APIDevAudio())
        print("EthName: %s" % stageBResult.EthName())
        print("AltName: %s" % stageBResult.AltName())
        print("WebHubVideo: %s" % stageBResult.WebHubVideo())
        print("Copyright_Video: %s" % stageBResult.Copyright_Video())
        print("APIDevVideo: %s" % stageBResult.APIDevVideo())

        print("==================================================================================")
        print("")

if __name__ == '__main__':
    unittest.main(argv=['test'], exit=False)
