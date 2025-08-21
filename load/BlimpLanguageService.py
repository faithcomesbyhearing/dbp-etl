from SQLUtility import SQLUtility
from Config import Config
from typing import List

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


    def getGospelsAndApostolicHistoryMap(self):
        sql = SQLUtility(self.config)
        result = sql.selectMap("SELECT id, notes FROM books WHERE book_group IN ('Gospels', 'Apostolic History')", None)

        # Validate and sanitize the result
        validated_result = {}
        for book_id, notes in result.items():
            if not book_id or not isinstance(book_id, str):
                continue
            if not notes or not isinstance(notes, str):
                notes = "No notes available"  # Default value for missing or invalid notes
            validated_result[book_id] = notes.strip()

        if not validated_result:
            raise ValueError("No valid data found for Gospels and Apostolic History mapping.")

        return validated_result

    def get_files_by_fileset_id(self, fileset_id: str) -> List[tuple[int, str]]:
        """
        Return a list of (file_id, file_name) for all files belonging to the given fileset.
        """
        sql = SQLUtility(self.config)
        query = """
            SELECT
                bfl.id,
                bfl.file_name
            FROM bible_filesets AS bf
            JOIN bible_files AS bfl
            ON bf.hash_id = bfl.hash_id
            WHERE bf.id = %s
        """

        rows = sql.select(query, (fileset_id,))

        return [(int(file_id), file_name) for file_id, file_name in rows]

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


def get_published_snm_by_stocknumber(stocknumber: str) -> int:
    config = Config()
    sql = SQLUtility(config)

    # All records with the same stocknumber should have the same published_snm.
    # If they don't, and at least one record has a value of 1, all records will be set to 1.
    # Therefore, this method will return 1 if at least one record has a published_snm of 1.
    result = sql.select(
        """SELECT CASE 
           WHEN MAX(bf.published_snm) = 1 THEN 1 
           ELSE 0 
           END as published_snm
        FROM bible_filesets bf
        INNER JOIN bible_fileset_tags bft ON bft.hash_id = bf.hash_id
        WHERE bft.name = 'stock_no'
        AND bf.archived IS FALSE
        AND bft.description = %s
        """,
        (stocknumber,)
    )
    
    return result[0][0] if result and len(result) > 0 else 0
