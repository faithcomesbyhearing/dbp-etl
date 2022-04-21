import os
from LPTSExtractReader import LPTSExtractReader
from StageCLanguageReader import *


class LanguageReaderCreator:
    # for backward compat on Stage B only    
    def createWithPath(self, path):
        return LPTSExtractReader(path)

    def create(self, config):
        stage = os.environ.get('DATA_MODEL_MIGRATION_STAGE', "B")   
        print("migration stage: [%s]" % stage)

        if (stage == "B"):
            return LPTSExtractReader(config.filename_lpts_xml)
        elif (stage == "C"):
            return StageCLanguageReader()
        else:
            print ("unrecognized stage: ", stage)


if (__name__ == '__main__'):
    from Config import *
    # export DATA_MODEL_MIGRATION_STAGE="C"
    languageReader = LanguageReaderCreator().create(Config())
    languageReader.getByStockNumber("abd")