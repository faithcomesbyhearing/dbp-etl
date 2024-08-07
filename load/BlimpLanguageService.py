from SQLUtility import SQLUtility
from Config import Config

class BlimpLanguageService:
    def __init__(self):
        self.config = Config()
    
    def getMediaById(self, mediaId):
        # self.config.setCurrentDatabaseDBName('LANGUAGE')
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT bf.id,\
                    bft.description,\
                    bfc.bible_id AS bible_id\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_tags bft on bft.hash_id = bf.hash_id\
            WHERE bft.name = 'stock_no'\
            AND bf.id = %s LIMIT 1\
            ",
            (mediaId)
        )

    def getMediaByStocknumber(self, stockNumber):
        # self.config.setCurrentDatabaseDBName('LANGUAGE')
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT bf.id,\
                    bft.description,\
                    bfc.bible_id AS bible_id,\
                    bfm.name AS mode_type\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_tags bft on bft.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_types bfty ON bfty.set_type_code = bf.set_type_code\
            INNER JOIN bible_fileset_modes bfm ON bfm.id = bfty.mode_id\
            WHERE bft.name = 'stock_no'\
            AND bft.description = %s LIMIT 1\
            ",
            (stockNumber)
        )

        # return  result[0] if len(result) > 0 else None

    def getMediaIdFromFilesetId(self, filesetId):
        # self.config.setCurrentDatabaseDBName('BIBLEBRAIN')
        sql = SQLUtility(self.config)

        fileset = sql.select(
            "SELECT bf.id,\
                bfm.name AS mode_type\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_types bfty ON bfty.set_type_code = bf.set_type_code\
            INNER JOIN bible_fileset_modes bfm ON bfm.id = bfty.mode_id\
            WHERE bf.id = %s limit 1\
            ",
            (filesetId)
        )

        mediaId = None
        modeType = None

        for rowTuple in fileset:
            mediaId = rowTuple[0]
            modeType = rowTuple[1]

        return (mediaId, modeType)

def getMediaByIdAndFormat(bibleId, format):
    config = Config()
    # config.setCurrentDatabaseDBName('LANGUAGE')
    sqlClient = SQLUtility(config)
    derivativeOfmediaRecord = sqlClient.select(
        "SELECT bf.id, 'Complete' as status\
        FROM bible_filesets bf\
        INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
        INNER JOIN bible_fileset_types bft ON bft.set_type_code = bf.set_type_code\
        INNER JOIN bible_fileset_modes bfm ON bfm.id = bft.mode_id\
        WHERE CHAR_LENGTH(bf.id) > 6\
        AND bfc.bible_id = %s\
        AND bfm.name = %s\
        ", (bibleId, format)
    )

    record = []
    for row in derivativeOfmediaRecord:
        damId = row[0]
        status = row[1]
        status = 'Live' if status == 'Complete' else '' 
        # record.append((damId, 1, status, 'No Fieldname'))
        record.append((damId, 1, status))

    return record