from LPTSExtractReader import LPTSExtractReader
from StageCLanguageReader import *


class LanguageReaderCreator:

    def __init__(self, stage = "B"):
        self.stage = stage
    

    def create(self, lpts_xml_path = None):
        print("migration stage: [%s]" % self.stage)

        if (self.stage == "B"):
            return LPTSExtractReader(lpts_xml_path)
        elif (self.stage == "C"):
            return StageCLanguageReader()
        else:
            print ("unrecognized stage: ", self.stage)


if (__name__ == '__main__'):
    from Config import *    
    languageReader = LanguageReaderCreator("B").create(Config().filename_lpts_xml)
    #languageReader = LanguageReaderCreator("C").create()
    languageReader.getByStockNumber("abd")

# python3 load/LanguageReaderCreator.py test