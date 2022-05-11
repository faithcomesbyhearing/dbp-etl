## UpdateDBPLanguageTranslation
import os
import io
import sys
from datetime import datetime
from Config import *
from SQLUtility import *
from SQLBatchExec import *

class UpdateDBPLanguageTranslation:
    def __init__(self, config, db, dbOut, languageReader):
        self.config = config
        self.db = db
        self.dbOut = dbOut
        self.languageReader = languageReader

    def updateOrInsertlanguageTranslation(self):
        return self.process()

    def process(self):
        bibles = self.languageReader.getBibleIdMap()
        langProcessed = {}

        for bibleId in bibles:
            for index, languageRecord in bibles.get(bibleId):
                lang = self.languageId(bibleId, languageRecord)
                insertRows = []
                updateRows = []

                if lang != None:
                    if languageRecord.HeartName() != None:
                        languageSourceId = lang
                        languageTranslationId = lang
                        languageName = languageRecord.HeartName()
                    else:
                        languageSourceId = lang
                        languageTranslationId = 6414
                        languageName = languageRecord.AltName()

                if languageName != None and langProcessed.get(lang) == None:
                    # store an index to avoid to process twice the same record
                    langProcessed[lang] = True
                    languageName = languageName.replace("'", "\\'")
                    # The column name that belongs to language_translations entity is a varchar with limit of 80 characters
                    languageName = languageName[:80]
                    langExistsByNameAndId = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, 9)

                    # Validate if the translation already exists
                    if langExistsByNameAndId == None:
                        translationIdWithSameLangAndPriority = self.getTranslationByLangIdAndPriority(languageSourceId, languageTranslationId, 9)
                        translationIdWithSameLangAndName = self.getTranslationByLangName(languageName, languageSourceId, languageTranslationId)

                        tableName = "language_translations"

                        # if it exists a translation record with the same Id and same priority but with different name
                        # and it also exists a other translation record with the same name and the same Id
                        if translationIdWithSameLangAndPriority != None and translationIdWithSameLangAndName != None:
                            attrNames = ("priority",)
                            pkeyNames = ("language_source_id", "language_translation_id", "priority")
                            updateRows.append((0, languageSourceId, languageTranslationId, 9))
                            self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)

                            updateRows = []
                            attrNames = ("priority",)
                            pkeyNames = ("language_source_id", "language_translation_id", "name")
                            updateRows.append((9, languageSourceId, languageTranslationId, languageName))
                            self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
                        elif translationIdWithSameLangAndPriority != None:
                            attrNames = ("name",)
                            pkeyNames = ("language_source_id", "language_translation_id", "priority", "id")
                            updateRows.append((languageName, languageSourceId, languageTranslationId, 9, translationIdWithSameLangAndPriority))
                            self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
                        elif translationIdWithSameLangAndName != None:
                            attrNames = ("name", "priority")
                            pkeyNames = ("language_source_id", "language_translation_id", "id")
                            updateRows.append((languageName, 9, languageSourceId, languageTranslationId, translationIdWithSameLangAndName))
                            self.dbOut.update(tableName, pkeyNames, attrNames, updateRows)
                        else:
                            attrNames = ("name", "priority")
                            pkeyNames = ("language_source_id", "language_translation_id")
                            insertRows.append((languageName, 9, languageSourceId, languageTranslationId))
                            self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
        return True

    def languageId(self, bibleId, bible):
        result = None
        if bible != None:
            iso = bible.ISO()
            langName = bible.LangName()
            result = self.db.selectScalar("SELECT id FROM languages WHERE iso=%s AND name=%s", (iso, langName))
            if result != None:
                return result
        else:
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
