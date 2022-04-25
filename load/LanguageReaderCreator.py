from LPTSExtractReader import LPTSExtractReader
from stagec.StageCLanguageReader import *


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
    config = Config()
    migration_stage = os.getenv("DATA_MODEL_MIGRATION_STAGE") # Should be "B" or "C"
    migration_stage = migration_stage if migration_stage != None else "B"
    languageReader = LanguageReaderCreator(migration_stage).create(config.filename_lpts_xml)
    results = languageReader.getByStockNumber("N1SPA/PDT")
    print("Reg_StockNumber [%s]" % results.Reg_StockNumber())
