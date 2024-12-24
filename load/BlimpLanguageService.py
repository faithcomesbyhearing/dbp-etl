from SQLUtility import SQLUtility
from Config import Config

class BlimpLanguageService:
    def __init__(self):
        self.config = Config()
    
    def getMediaById(self, mediaId):
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT bf.id,\
                    bft.description,\
                    bfc.bible_id AS bible_id,\
                    bfm.name AS mode_type,\
                    bt.name AS bible_name\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
            INNER JOIN bibles b ON b.id = bfc.bible_id\
            INNER JOIN bible_fileset_tags bft on bft.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_types bfty ON bfty.set_type_code = bf.set_type_code\
            INNER JOIN bible_fileset_modes bfm ON bfm.id = bfty.mode_id\
            LEFT JOIN bible_translations bt ON b.id = bt.bible_id AND bt.language_id = 6414\
            WHERE bft.name = 'stock_no'\
            AND bf.id = %s LIMIT 1\
            ",
            (mediaId)
        )

    def getMediaByStocknumber(self, stockNumber):
        sql = SQLUtility(self.config)

        return sql.select(
            "SELECT bf.id,\
                    bft.description,\
                    bfc.bible_id AS bible_id,\
                    bfm.name AS mode_type,\
                    bt.name AS bible_name\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
            INNER JOIN bibles b ON b.id = bfc.bible_id\
            INNER JOIN bible_fileset_tags bft on bft.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_types bfty ON bfty.set_type_code = bf.set_type_code\
            INNER JOIN bible_fileset_modes bfm ON bfm.id = bfty.mode_id\
            LEFT JOIN bible_translations bt ON b.id = bt.bible_id AND bt.language_id = 6414\
            WHERE bft.name = 'stock_no'\
            AND bft.description = %s LIMIT 1\
            ",
            (stockNumber)
        )

    def getMediaIdFromFilesetId(self, filesetId):
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

    def getStocknumber(self, stocknumber):
        sql = SQLUtility(self.config)

        return sql.selectScalar("\
            SELECT bft.description\
            FROM bible_filesets bf\
            INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
            INNER JOIN bible_fileset_tags bft on bft.hash_id = bf.hash_id\
            WHERE bft.name = 'stock_no'\
            AND bft.description = %s\
            GROUP BY bft.description\
            LIMIT 1\
            ",
            (stocknumber)
        )


# BWF note: this is only called for text processing
def getMediaByIdAndFormat(bibleId, format):
    config = Config()
    # config.setCurrentDatabaseDBName('LANGUAGE')
    sqlClient = SQLUtility(config)
    # BWF 10/28/24. Changed CHAR_LENGTH(bf.id) from > 6 to =10. This filters out the derivatives, which are now in the database before loading.
    # but it introduces a requirement that there be a ten character text fileset, and not just the old six character text fileset.
    derivativeOfmediaRecord = sqlClient.select(
        "SELECT bf.id, 'Complete' as status\
        FROM bible_filesets bf\
        INNER JOIN bible_fileset_connections bfc ON bfc.hash_id = bf.hash_id\
        INNER JOIN bible_fileset_types bft ON bft.set_type_code = bf.set_type_code\
        INNER JOIN bible_fileset_modes bfm ON bfm.id = bft.mode_id\
        WHERE CHAR_LENGTH(bf.id) = 10\
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

def getLicensorsByFilesetId(filesetId):
    config = Config()
    sqlClient = SQLUtility(config)

    licensors = sqlClient.select(
        "SELECT organizations.id, organization_translations.name, organization_logos.url AS logo\
        FROM organizations\
        INNER JOIN organization_translations ON organization_translations.organization_id = organizations.id\
        LEFT JOIN organization_logos ON organization_logos.organization_id = organizations.id AND organization_logos.icon = false\
        INNER JOIN bible_fileset_copyright_organizations ON bible_fileset_copyright_organizations.organization_id = organizations.id\
        INNER JOIN bible_filesets ON bible_filesets.hash_id = bible_fileset_copyright_organizations.hash_id\
        WHERE organization_translations.language_id = 6414\
        AND bible_fileset_copyright_organizations.organization_role = 2\
        AND bible_filesets.id = %s\
        GROUP BY organizations.id, organization_translations.name, organization_logos.url\
        ", (filesetId)
    )

    records = []
    for row in licensors:
        organizationId = row[0]
        organizationName = row[1]
        organizationLogo = row[2]
        records.append((organizationId, organizationName, organizationLogo))

    return records

def getCopyrightByFilesetId(filesetId):
    config = Config()
    sqlClient = SQLUtility(config)

    licensors = sqlClient.select(
        "SELECT bible_fileset_copyrights.copyright, bible_fileset_copyrights.copyright_date, bible_fileset_copyrights.copyright_description\
        FROM bible_fileset_copyrights\
        INNER JOIN bible_filesets ON bible_filesets.hash_id = bible_fileset_copyrights.hash_id\
        AND bible_filesets.id = %s\
        ", (filesetId)
    )

    records = []
    for row in licensors:
        copyright = row[0]
        copyrightDate = row[1]
        copyrightDescription = row[2]
        records.append((copyright, copyrightDate, copyrightDescription))

    return records
