import os
from LPTSExtractReader import LPTSExtractReader
from StageCLanguageReader import StageCLanguageReader


class LanguageReaderStageTest:
    def __init__(self, config):
        self.LPTSReader = LPTSExtractReader(config.filename_lpts_xml)
        self.languageReader = StageCLanguageReader()

    def TestGetByStockNumber(self, stockNumber):
        print("Testing getByStockNumber(%s)" % stockNumber)
        resultStageB = self.LPTSReader.getByStockNumber(stockNumber)
        resultStageC = self.languageReader.getByStockNumber(stockNumber)

        print(resultStageB)
        print(resultStageC)

if (__name__ == '__main__'):
    from Config import sys, Config

    if len(sys.argv) < 2:
        print("FATAL command line parameters: stockNumber ")
        sys.exit()

    stockNumber = sys.argv[2]
    stageTester = LanguageReaderStageTest(Config())
    stageTester.TestGetByStockNumber(stockNumber)
