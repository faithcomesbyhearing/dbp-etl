# BibleBrainLoadController.py

import os
from Config import *
from RunStatus import *
from LPTSExtractReader import *
from Log import *
from InputFileset import *
from Validate import *
from S3Utility import *
from SQLBatchExec import *
from UpdateDBPFilesetTables import *
from UpdateDBPBiblesTable import *
from UpdateDBPLPTSTable import *
from UpdateDBPVideoTables import *
from UpdateDBPBibleFilesSecondary import *


class BibleBrainLoadController:
    def __init__(self, config):
        self.config = config

        self.config.setCurrentDatabaseDBName('BIBLEBRAIN')
        self.dbBiblebrain = SQLUtility(config)
        self.config.setCurrentDatabaseDBName('LANGUAGE')
        self.dbLanguage = SQLUtility(config)

    def getFilesets(self):
        resultSet = self.dbBiblebrain.select("SELECT * FROM fileset LIMIT 10", ())
        for row in resultSet:
            print("Get fileset %s" % row[0], row)

    def updateFilesetTables(self, inputFilesets):
        inp = inputFilesets
        dbOut = SQLBatchExec(self.config)
        update = UpdateDBPFilesetTables(self.config, self.db, dbOut)

if (__name__ == '__main__'):
    config = Config()
    # AWSSession.shared() # ensure AWSSession init
    db = SQLUtility(config)
    ctrl = BibleBrainLoadController(config)
    ctrl.getFilesets()

    # RunStatus.exit()
