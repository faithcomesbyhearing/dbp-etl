import os
from LPTSExtractReader import LPTSExtractReader


class LanguageReaderCreator:
    # for backward compat on Stage B only    
    def createWithPath(self, path):
        return LPTSExtractReader(path)

    def create(self, config):
        stage = os.environ.get('DATA_MODEL_MIGRATION_STAGE', "B")   
        print("migration stage: [%s]" % stage)

        if (stage == "B"):
            return LPTSExtractReader(config.filename_lpts_xml)


if (__name__ == '__main__'):
    from Config import *
    languageReader = LanguageReaderCreator().create(Config())
    languageReader.getByStockNumber("abd")