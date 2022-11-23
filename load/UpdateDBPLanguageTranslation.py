## UpdateDBPLanguageTranslation
import os
import io
import sys
from datetime import datetime
from Config import *
from SQLUtility import *
from SQLBatchExec import *

class UpdateDBPLanguageTranslation:
    HIGH_PRIORITY = 9
    ALT_NAME_PRIORITY = 0
    LANGUAGE_UNDETERMINED_TRANSLATION_ID = 8012
    LANGUAGE_ENGLISH_ID = 6414

    def __init__(self, config, db, dbOut, languageReader):
        self.config = config
        self.db = db
        self.dbOut = dbOut
        self.languageReader = languageReader
        self.langProcessed = {}

    def updateOrInsertlanguageTranslation(self):
        return self.process()

    def process(self):
        languageRecords = self.languageReader.resultSet

        for languageRecord in languageRecords:
            lang = self.languageId(languageRecord)

            if lang != None:
                self._populatePriorityNine(lang, languageRecord)

                if languageRecord.AltName() != None:
                    self._processLanguageWithAltName(lang, languageRecord)

        return True

    def _populatePriorityNine(self, languageSourceId, languageRecord):
        if languageRecord.LangName() != None:
            priorityEngLanguageName = [languageRecord.LangName().strip()]
            self._updateOrInsertLanguageTranslations(
                priorityEngLanguageName,
                languageSourceId,
                self.LANGUAGE_ENGLISH_ID,
                self.HIGH_PRIORITY
            )

            priorityLanguageSourceName = [languageRecord.HeartName().strip()] if languageRecord.HeartName() != None else priorityEngLanguageName
            self._updateOrInsertLanguageTranslations(
                priorityLanguageSourceName,
                languageSourceId,
                languageSourceId,
                self.HIGH_PRIORITY
            )

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

                indexedKey = "%s%s%s%s" % (languageSourceId, languageTranslationId, languageName.lower(), priority)

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

    def languageId(self, languageRecord):
        result = None
        if languageRecord != None:
            iso = languageRecord.ISO()
            langName = languageRecord.LangName()

            if iso != None and langName != None:
                result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
                if result != None:
                    return result

            if iso != None:
                result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
                if result != None:
                    return result

            bibleId = languageRecord.DBP_EquivalentSet()
            if bibleId != None:
                iso = bibleId[:3].lower()

                result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s", (iso))
                if result != None:
                    return result

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
