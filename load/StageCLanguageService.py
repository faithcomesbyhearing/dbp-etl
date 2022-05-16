from SQLUtility import *
from Config import *

class StageCLanguageService:
    def __init__(self):
        self.config = Config()

    def getAllMediaRecords(self):
        self.config.setCurrentDatabaseDBName('LANGUAGE')
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT md.Id,\
                    md.stocknum,\
                    md.extent,\
                    flc.countryName AS country_name,\
                    bt.acronynm AS bible_acronynm,\
                    bt.english_name AS bible_translation_english_name,\
                    bm.bibleId AS bible_id,\
                    fl.iso3,\
                    fl.name,\
                    ls.statusName,\
                    md.derivativeOf,\
                    scod.english_name AS orthography,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'ethname' limit 1) as lang_ethname,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'alternate' limit 1) as lang_alternate\
            FROM media md\
            INNER JOIN bible_media bm ON md.id = bm.mediaId\
            INNER JOIN bibleTranslation bt ON md.translationId = bt.Id\
            LEFT JOIN fcbhLanguage fl ON bt.languageId = fl.Id\
            LEFT JOIN EXTERNAL.script_codes scod ON md.scriptCode = scod.code\
            LEFT JOIN language_status ls ON bt.languageId = ls.languageId\
            LEFT JOIN fcbhLanguage_country flc ON bt.languageId = flc.languageId AND md.countryName = flc.countryName\
            ORDER BY bm.bibleId\
            ", None
        )
    
    def getMediaById(self, mediaId):
        self.config.setCurrentDatabaseDBName('LANGUAGE')
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT md.Id,\
                    md.stocknum,\
                    md.extent,\
                    flc.countryName AS country_name,\
                    bt.acronynm AS bible_acronynm,\
                    bt.english_name AS bible_translation_english_name,\
                    bm.bibleId AS bible_id,\
                    fl.iso3,\
                    fl.name,\
                    ls.statusName,\
                    md.derivativeOf,\
                    scod.english_name AS orthography,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'ethname' limit 1) as lang_ethname,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'alternate' limit 1) as lang_alternate\
            FROM media md\
            INNER JOIN bible_media bm ON md.id = bm.mediaId\
            INNER JOIN bibleTranslation bt ON md.translationId = bt.Id\
            LEFT JOIN fcbhLanguage fl ON bt.languageId = fl.Id\
            LEFT JOIN EXTERNAL.script_codes scod ON md.scriptCode = scod.code\
            LEFT JOIN language_status ls ON bt.languageId = ls.languageId\
            LEFT JOIN fcbhLanguage_country flc ON bt.languageId = flc.languageId AND md.countryName = flc.countryName\
            WHERE md.Id = %s limit 1\
            ",
            (mediaId)
        )

    def getMediaByStocknumber(self, stockNumber):
        self.config.setCurrentDatabaseDBName('LANGUAGE')
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT md.Id,\
                    md.stocknum,\
                    md.extent,\
                    flc.countryName AS country_name,\
                    bt.acronynm AS bible_acronynm,\
                    bt.english_name AS bible_translation_english_name,\
                    bm.bibleId AS bible_id,\
                    fl.iso3,\
                    fl.name,\
                    ls.statusName,\
                    md.derivativeOf,\
                    scod.english_name AS orthography,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'ethname' limit 1) as lang_ethname,\
                    (select fln.name from fcbhLanguageName fln WHERE fln.languageId = fl.id and fln.type = 'alternate' limit 1) as lang_alternate\
            FROM media md\
            INNER JOIN bible_media bm ON md.id = bm.mediaId\
            INNER JOIN bibleTranslation bt ON md.translationId = bt.Id\
            LEFT JOIN fcbhLanguage fl ON bt.languageId = fl.Id\
            LEFT JOIN EXTERNAL.script_codes scod ON md.scriptCode = scod.code\
            LEFT JOIN language_status ls ON bt.languageId = ls.languageId\
            LEFT JOIN fcbhLanguage_country flc ON bt.languageId = flc.languageId AND md.countryName = flc.countryName\
            WHERE stocknum = %s limit 1\
            ",
            (stockNumber)
        )

        return  result[0] if len(result) > 0 else None

    def getMediaIdFromFilesetId(self, filesetId):
        self.config.setCurrentDatabaseDBName('BIBLEBRAIN')
        sql = SQLUtility(self.config)

        fileset = sql.select(
            "SELECT fs.mediaId\
            FROM fileset fs\
            WHERE fs.Id = %s limit 1\
            ",
            (filesetId)
        )

        mediaId = None

        for rowTuple in fileset:
            mediaId = rowTuple[0]

        return mediaId

def getMediaByIdAndFormat(mediaId, format):
    config = Config()
    config.setCurrentDatabaseDBName('LANGUAGE')
    sqlClient = SQLUtility(config)
    derivativeOfmediaRecord = sqlClient.select(
        "SELECT md.Id, ls.statusName\
        FROM media md\
        INNER JOIN bible_media bm ON md.id = bm.mediaId\
        INNER JOIN bibleTranslation bt ON md.translationId = bt.Id\
        LEFT JOIN language_status ls ON bt.languageId = ls.languageId\
        WHERE md.Id = %s\
        AND md.format = %s\
        LIMIT 1\
        ", (mediaId, format)
    )

    record = []
    for row in derivativeOfmediaRecord:
        damId = row[0]
        status = row[1]
        status = 'Live' if status == 'Complete' else '' 
        record.append((damId, 1, status, 'No Fieldname'))

    return record
