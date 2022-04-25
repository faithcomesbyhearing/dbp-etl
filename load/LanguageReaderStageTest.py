import os
from LPTSExtractReader import LPTSExtractReader
from stagec.StageCLanguageReader import StageCLanguageReader


class LanguageReaderStageTest:
    def __init__(self, config):
        self.LPTSReader = LPTSExtractReader(config.filename_lpts_xml)
        self.languageReader = StageCLanguageReader()

    def TestGetBibleIdMap(self):
        listStageC = self.languageReader.getBibleIdMap()
        listStageB = self.LPTSReader.getBibleIdMap()
        print("========================== START - GetBibleIdMap =================================")
        print("==================================================================================")

        print("listStageC len(%s) " % len(listStageC))
        print("listStageB len(%s) " % len(listStageB))

        print("listStageC list of SPAPDT len(%s)" % len(listStageC.get('SPAPDT')))
        print("listStageB list of SPAPDT len(%s)" % len(listStageB.get('SPAPDT')))

        print("=========================== END - GetBibleIdMap ==================================")
        print("==================================================================================")

    def TestGetByStockNumber(self, stockNumber):
        print("========================== START - GetByStockNumber ==============================")
        print("==================================================================================")
        stockNumberWithFormat = self.LPTSReader.getStocknumberWithFormat(stockNumber)
        print("Testing B getByStockNumber(%s)" % stockNumberWithFormat)
        resultStageB = self.LPTSReader.getByStockNumber(stockNumberWithFormat)
        stockNumberWithFormat = self.languageReader.getStocknumberWithFormat(stockNumber)
        print("Testing C getByStockNumber(%s)" % stockNumberWithFormat)
        resultStageC = self.languageReader.getByStockNumber(stockNumberWithFormat)

        self.printResultStageC(resultStageC)
        self.printResultStageB(resultStageB)

        print("=========================== END - GetByStockNumber ===============================")
        print("==================================================================================")

    def TestGetFilesetRecords10(self, damId):
        print("")
        print("========================== START - GetFilesetRecords10 ===========================")
        print("==================================================================================")
        resultStageB = self.LPTSReader.getFilesetRecords10(damId)
        resultStageC = self.languageReader.getFilesetRecords10(damId)

        print("resultStageC ============> set", resultStageC)
        print("resultStageB ============> set", resultStageB)
        # self.printResultStageC(resultStageC)
        # self.printResultStageB(resultStageB)

        for (languageRecord, status, key) in resultStageB:
            self.printResultStageB(languageRecord)
        for (languageRecord, status, key) in resultStageC:
            self.printResultStageC(languageRecord)


        print("========================== END - GetFilesetRecords10 =============================")
        print("==================================================================================")

    def printResultStageC(self, resultStageC):
        print("")
        print("========================== RESULT STAGE C ========================================")
        print(resultStageC)
        print("Reg_StockNumber: %s" % resultStageC.Reg_StockNumber())
        print("LangName: %s" % resultStageC.LangName())
        print("ISO: %s" % resultStageC.ISO())
        print("Country: %s" % resultStageC.Country())
        print("DBP_Equivalent: %s" % resultStageC.DBP_Equivalent())
        print("Volumne_Name: %s" % resultStageC.Volumne_Name())

        print("==================================================================================")
        print("")

    def printResultStageB(self, resultStageB):
        print("")
        print("========================== RESULT STAGE B ========================================")
        print(resultStageB)
        print("Reg_StockNumber: %s" % resultStageB.Reg_StockNumber())
        print("LangName: %s" % resultStageB.LangName())
        print("ISO: %s" % resultStageB.ISO())
        print("Country: %s" % resultStageB.Country())
        print("DBP_Equivalent: %s" % resultStageB.DBP_Equivalent())
        print("Licensor: %s" % resultStageB.Licensor())
        print("Copyrightc: %s" % resultStageB.Copyrightc())
        print("Copyrightp: %s" % resultStageB.Copyrightp())
        print("Volumne_Name: %s" % resultStageB.Volumne_Name())
        print("DBPMobile: %s" % resultStageB.DBPMobile())
        print("DBPWebHub: %s" % resultStageB.DBPWebHub())
        print("MobileText: %s" % resultStageB.MobileText())
        print("HubText: %s" % resultStageB.HubText())
        print("APIDevText: %s" % resultStageB.APIDevText())
        print("APIDevAudio: %s" % resultStageB.APIDevAudio())
        print("EthName: %s" % resultStageB.EthName())
        print("AltName: %s" % resultStageB.AltName())
        print("WebHubVideo: %s" % resultStageB.WebHubVideo())
        print("Copyright_Video: %s" % resultStageB.Copyright_Video())
        print("APIDevVideo: %s" % resultStageB.APIDevVideo())

        print("==================================================================================")
        print("")

if (__name__ == '__main__'):
    from Config import sys, Config

    if len(sys.argv) < 2:
        print("FATAL command line parameters: stockNumber ")
        sys.exit()

    print(sys.argv)
    stockNumber = sys.argv[2]
    stageTester = LanguageReaderStageTest(Config())
    stageTester.TestGetByStockNumber(stockNumber)
    stageTester.TestGetFilesetRecords10('GNDWYIN2DA')
    stageTester.TestGetBibleIdMap()
