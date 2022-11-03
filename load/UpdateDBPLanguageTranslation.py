## UpdateDBPLanguageTranslation
import os
import io
import sys
from datetime import datetime
from Config import *
from SQLUtility import *
from SQLBatchExec import *

class UpdateDBPLanguageTranslation:
    HEART_NAME_PRIORITY = 9
    ALT_NAME_PRIORITY = 0
    LANGUAGE_UNDETERMINED_TRANSLATION_ID = 8012

    def __init__(self, config, db, dbOut, languageReader):
        self.config = config
        self.db = db
        self.dbOut = dbOut
        self.languageReader = languageReader
        self.langProcessed = {}

    def updateOrInsertlanguageTranslation(self):
        return self.process()

    def process(self):
        bibles = self.languageReader.getBibleIdMap()

        for bibleId in bibles:
            for _, languageRecord in bibles.get(bibleId):
                lang = self.languageId(bibleId, languageRecord)

                if lang != None:
                    if languageRecord.HeartName() != None:
                        self._processLanguageWithHeartName(lang, languageRecord)

                    if languageRecord.AltName() != None:
                        self._processLanguageWithAltName(lang, languageRecord)

        return True

    def _processLanguageWithHeartName(self, languageSourceId, languageRecord):
        heartNamePriority = self.HEART_NAME_PRIORITY
        languageTranslationId = languageSourceId
        languageNames = [languageRecord.HeartName()]
        self._updateOrInsertLanguageTranslations(languageNames, languageSourceId, languageTranslationId, heartNamePriority)

    def _processLanguageWithAltName(self, languageSourceId, languageRecord):
        altNamePriority = self.ALT_NAME_PRIORITY
        languageTranslationId = self.LANGUAGE_UNDETERMINED_TRANSLATION_ID
        languageNames = languageRecord.AltNameList()
        self._updateOrInsertLanguageTranslations(languageNames, languageSourceId, languageTranslationId, altNamePriority)

    def _updateOrInsertLanguageTranslations(self, languageNames, languageSourceId, languageTranslationId, priority):
        for languageName in languageNames:
            if languageName != None and languageName != "":
                insertRows = []
                updateRows = []

                indexedKey = "%s%s%s" % (languageSourceId,languageName.lower(),priority)

                if self.langProcessed.get(indexedKey) == None and not self._isPejorativeAltName(languageName):
                    # store an index to avoid to process twice the same record
                    self.langProcessed[indexedKey] = True
                    # The column name that belongs to language_translations entity is a varchar with limit of 80 characters
                    languageName = languageName[:80]

                    translationIdWithSameLangAndName = self.getTranslationByLangName(languageName, languageSourceId, languageTranslationId)

                    tableName = "language_translations"

                    if translationIdWithSameLangAndName != None:
                        langExistsByNameAndId = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, priority)

                        if langExistsByNameAndId == None:
                            languageName = languageName.replace("'", "\\'")
                            attrNames = ("priority",)
                            pkeyNames = ("language_source_id", "language_translation_id", "name", "id")
                            updateRows.append((priority, languageSourceId, languageTranslationId, languageName, translationIdWithSameLangAndName))
                            self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
                    else:
                        languageName = languageName.replace("'", "\\'")
                        attrNames = ("name", "priority")
                        pkeyNames = ("language_source_id", "language_translation_id")
                        insertRows.append((languageName, priority, languageSourceId, languageTranslationId))
                        self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)

    def _isPejorativeAltName(self, languageName):
        return languageName.find("(pej") != -1

    def languageId(self, bibleId, bible):
        result = None
        if bible != None:
            iso = bible.ISO()
            langName = bible.LangName()

            if iso != None and langName != None:
                result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
                if result != None:
                    return result

        iso = bibleId[:3].lower()

        result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
        if result != None:
            return result
        else:
            return None

    def getTranslationByLangIdAndPriority(self, languageSourceId, languageTranslationId, priority):
        return self.db.selectScalar("SELECT id FROM language_translations WHERE language_source_id=%s AND language_translation_id = %s AND priority=%s LIMIT 1", (languageSourceId, languageTranslationId, priority))

    def getTranslationByLangName(self, name, languageSourceId, languageTranslationId):
        return self.db.selectScalar("SELECT id FROM language_translations WHERE language_source_id=%s AND language_translation_id = %s AND name=%s LIMIT 1", (languageSourceId, languageTranslationId, name))

    def getTranslationByLangNameIdAndPriority(self, name, languageSourceId, languageTranslationId, priority):
        return self.db.selectScalar("SELECT id FROM language_translations WHERE language_source_id=%s AND language_translation_id = %s AND name=%s AND priority=%s LIMIT 1", (languageSourceId, languageTranslationId, name, priority))

## Unit Test
if (__name__ == '__main__'):
    from LanguageReader import *
    from LanguageReaderCreator import LanguageReaderCreator

    config = Config()
    db = SQLUtility(config)
    dbOut = SQLBatchExec(config)
    migration_stage = "B" if os.getenv("DATA_MODEL_MIGRATION_STAGE") == None else os.getenv("DATA_MODEL_MIGRATION_STAGE")
    languageReader = LanguageReaderCreator(migration_stage).create(config.filename_lpts_xml)
    translations = UpdateDBPLanguageTranslation(config, db, dbOut, languageReader)
    translations.updateOrInsertlanguageTranslation()
    db.close()

    dbOut.displayStatements()
    dbOut.displayCounts()
    dbOut.execute("test-update-language-translations")
