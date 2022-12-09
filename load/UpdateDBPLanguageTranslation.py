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
    ETHNOLOGUE_NAME_PRIORITY = 5
    LANGUAGE_UNDETERMINED_TRANSLATION_ID = 8012
    LANGUAGE_ENGLISH_ID = 6414

    def __init__(self, config, db, dbOut, languageReader):
        self.config = config
        self.db = db
        self.dbOut = dbOut
        self.languageReader = languageReader
        self.langProcessed = {}
        self.langUpdated = {}

    def updateOrInsertlanguageTranslation(self):
        return self.process()

    def process(self):
        languageRecords = self.languageReader.resultSet

        for languageRecord in languageRecords:
            lang = self.languageId(languageRecord)

            if lang != None:
                self._populatePriorityNine(lang, languageRecord)

                if languageRecord.EthName() != None:
                    self._processLanguageEthName(lang, languageRecord)

                if languageRecord.AltName() != None:
                    self._processLanguageWithAltName(lang, languageRecord)

        return True

    def _populatePriorityNine(self, languageSourceId, languageRecord):
        if languageRecord.LangName() != None:
            priorityEngLanguageName = languageRecord.LangName().strip()
            self._updateOrInsertLangTranslationsHighPriority(
                priorityEngLanguageName,
                languageSourceId,
                self.LANGUAGE_ENGLISH_ID,
                self.HIGH_PRIORITY
            )

            priorityLanguageSourceName = languageRecord.HeartName().strip() if languageRecord.HeartName() != None else priorityEngLanguageName
            self._updateOrInsertLangTranslationsHighPriority(
                priorityLanguageSourceName,
                languageSourceId,
                languageSourceId,
                self.HIGH_PRIORITY
            )

    def _processLanguageWithAltName(self, languageSourceId, languageRecord):
        altNamePriority = self.ALT_NAME_PRIORITY
        languageTranslationId = self.LANGUAGE_UNDETERMINED_TRANSLATION_ID
        languageNames = languageRecord.AltNameList()
        # if the name in AltName matches the EthName, it should ignore the AltName
        if languageRecord.EthName() != None:
            ethName = languageRecord.EthName()
            languageNames[:] = (lang for lang in languageNames if lang != ethName)

        self._updateOrInsertLangTranslationsLowerPriority(languageNames, languageSourceId, languageTranslationId, altNamePriority)

    def _processLanguageEthName(self, languageSourceId, languageRecord):
        ethNamePriority = self.ETHNOLOGUE_NAME_PRIORITY
        languageTranslationId = self.LANGUAGE_UNDETERMINED_TRANSLATION_ID
        self._updateOrInsertLangTranslationsEthName(languageRecord.EthName(), languageSourceId, languageTranslationId, ethNamePriority)

    def _updateOrInsertLangTranslationsLowerPriority(self, languageNames, languageSourceId, languageTranslationId, priority):
        for languageName in languageNames:
            if languageName != None and languageName != "":
                insertRows = []
                updateRows = []

                indexedKey = "%s%s%s%s" % (languageSourceId, languageTranslationId, languageName.lower(), priority)
                indexedEthNameKey = "%s%s%s%s" % (languageSourceId, languageTranslationId, languageName.lower(), self.ETHNOLOGUE_NAME_PRIORITY)

                if self.langProcessed.get(indexedKey) == None and not self._isPejorativeAltName(languageName):
                    # store an index to avoid to process twice the same record
                    self.langProcessed[indexedKey] = True
                    # The column name that belongs to language_translations entity is a varchar with limit of 80 characters
                    languageName = languageName[:80]

                    row = self.getTranslationByLangName(languageName, languageSourceId, languageTranslationId)

                    tableName = "language_translations"

                    if row != None:
                        (translationIdWithSameLangAndName, oldPriority) = row
                        langExistsByNameAndId = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, priority)
                        langEthNameId = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, self.ETHNOLOGUE_NAME_PRIORITY)

                        if langExistsByNameAndId == None and langEthNameId == None and self.langUpdated.get(indexedEthNameKey) == None:
                            languageName = languageName.replace("'", "\\'")
                            pkeyNames = ("language_source_id", "language_translation_id", "name", "id")
                            updateRows.append(("priority", priority, oldPriority, languageSourceId, languageTranslationId, languageName, translationIdWithSameLangAndName))
                            self.dbOut.updateCol(tableName, pkeyNames, updateRows)
                            # store an index to avoid to update or insert twice the same record with the same unique key
                            self.langUpdated[indexedKey] = True
                    else:
                        if self.langUpdated.get(indexedEthNameKey) == None:
                            languageName = languageName.replace("'", "\\'")
                            attrNames = ("name", "priority")
                            pkeyNames = ("language_source_id", "language_translation_id")
                            insertRows.append((languageName, priority, languageSourceId, languageTranslationId))
                            self.dbOut.insert(tableName, pkeyNames, attrNames, insertRows)
                            # # store an index to avoid to update or insert twice the same record with the same unique key
                            self.langUpdated[indexedKey] = True

    def _updateOrInsertLangTranslationsEthName(self, languageName, languageSourceId, languageTranslationId, priority):
        if languageName != None and languageName != "":
            indexedKey = "%s%s%s%s" % (languageSourceId, languageTranslationId, languageName.lower(), priority)

            if self.langProcessed.get(indexedKey) == None and not self._isPejorativeAltName(languageName):
                # store an index to avoid to process twice the same record
                self.langProcessed[indexedKey] = True
                languageName = languageName[:80]

                langExistsByNameIdAndPriority = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, priority)

                if langExistsByNameIdAndPriority == None:
                    tableName = "language_translations"

                    rowOldEthName = self.getTranslationByLangIdAndPriority(languageSourceId, languageTranslationId, priority)
                    rowlangNoEthName = self.getTranslationByLangName(
                        languageName,
                        languageSourceId,
                        languageTranslationId
                    )

                    if rowOldEthName != None:
                        (langExistsByPriorityAndId, oldLangTranName) = rowOldEthName
                        pkeyNames = ("language_source_id", "language_translation_id", "priority", "id")
                        valuesToUpdate = [("name", languageName, oldLangTranName, languageSourceId, languageTranslationId, priority, langExistsByPriorityAndId)]
                        self.dbOut.updateCol(tableName, pkeyNames, valuesToUpdate)
                    elif rowlangNoEthName != None:
                        (langIdNoEthName, oldLangPriority) = rowlangNoEthName
                        pkeyNames = ("language_source_id", "language_translation_id", "id")
                        valuesToUpdate = [("priority", priority, oldLangPriority, languageSourceId, languageTranslationId, langIdNoEthName)]
                        self.dbOut.updateCol(tableName, pkeyNames, valuesToUpdate)
                    else:
                        languageName = languageName.replace("'", "\\'")
                        attrNames = ("name", "priority")
                        pkeyNames = ("language_source_id", "language_translation_id")
                        valuesToInsert  = [(languageName, priority, languageSourceId, languageTranslationId)]
                        self.dbOut.insert(tableName, pkeyNames, attrNames, valuesToInsert)

                    # store an index to avoid to update or insert twice the same record with the same unique key
                    self.langUpdated[indexedKey] = True

    def _updateOrInsertLangTranslationsHighPriority(self, languageName, languageSourceId, languageTranslationId, priority):
        if languageName != None and languageName != "":
            insertRows = []
            updateRows = []

            indexedKey = "%s%s%s" % (languageSourceId, languageTranslationId, priority)

            if self.langProcessed.get(indexedKey) == None:
                # store an index to avoid to process twice the same record
                self.langProcessed[indexedKey] = True
                # The column name that belongs to language_translations entity is a varchar with limit of 80 characters
                languageName = languageName[:80]

                langExistsByNameIdAndPriority = self.getTranslationByLangNameIdAndPriority(languageName, languageSourceId, languageTranslationId, priority)

                if langExistsByNameIdAndPriority == None:
                    tableName = "language_translations"

                    rowByPriority = self.getTranslationByLangIdAndPriority(languageSourceId, languageTranslationId, priority)
                    rowByLangName = self.getTranslationByLangName(languageName, languageSourceId, languageTranslationId)

                    languageName = languageName.replace("'", "\\'")

                    # There is Language with Priority 9 but Language Name is not equal
                    if rowByPriority != None and rowByLangName == None:
                        (langExistsByPriorityAndId, oldLangTranName) = rowByPriority

                        pkeyNames = ("language_source_id", "language_translation_id", "priority", "id")

                        updateRows.append(("name", languageName, oldLangTranName, languageSourceId, languageTranslationId, priority, langExistsByPriorityAndId))
                        self.dbOut.updateCol(tableName, pkeyNames, updateRows)
                    # There is Language with Priority 9 and there is another Language with the same Name
                    elif rowByPriority != None and rowByLangName != None:
                        (langExistsByPriorityAndId, oldLangTranName) = rowByPriority
                        (langExistsByNameAndId, oldPriority) = rowByLangName

                        if langExistsByPriorityAndId != langExistsByNameAndId:
                            pkeyNames = ("language_source_id", "language_translation_id", "id")
                            updateRows = [("priority", 0, priority, languageSourceId, languageTranslationId, langExistsByPriorityAndId)]
                            self.dbOut.updateCol(tableName, pkeyNames, updateRows)

                            pkeyNames = ("language_source_id", "language_translation_id", "name", "id")
                            updateRows = [("priority", priority, oldPriority, languageSourceId, languageTranslationId, languageName, langExistsByNameAndId)]
                            self.dbOut.updateCol(tableName, pkeyNames, updateRows)
                    # There is a Language with the same but different priority to 9
                    # elif langExistsByPriorityAndId == None and langExistsByNameAndId != None:
                    elif rowByPriority == None and rowByLangName != None:
                        (langExistsByNameAndId, oldPriority) = rowByLangName

                        pkeyNames = ("language_source_id", "language_translation_id", "name", "id")
                        updateRows = [("priority", priority, oldPriority, languageSourceId, languageTranslationId, languageName, langExistsByNameAndId)]
                        self.dbOut.updateCol(tableName, pkeyNames, updateRows)
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
        return self.db.selectRow("SELECT id, name FROM language_translations WHERE language_source_id=%s AND language_translation_id = %s AND priority=%s LIMIT 1", (languageSourceId, languageTranslationId, priority))

    def getTranslationByLangName(self, name, languageSourceId, languageTranslationId):
        return self.db.selectRow("SELECT id, priority FROM language_translations WHERE language_source_id=%s AND language_translation_id = %s AND name=%s LIMIT 1", (languageSourceId, languageTranslationId, name))

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
