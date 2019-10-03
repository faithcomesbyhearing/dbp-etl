#!/bin/sh -v -e

python3 py/BucketListingTable.py
# 1109357 9/24
# 1114097 9/24
# 1157283 9/27
# 30698082 9/27
# 1626574 10/1

python3 py/BiblesTable.py
# 1397 9/24
# 1398 9/24
# 1797 10/1

python3 py/BibleTranslationsTable.py
# 1397 9/24
# 1398 9/24
# ? 10/1

python3 py/BibleFilesetsTable.py
# 3728 9/24
# 3749 9/24
# 6480 10/1

python3 py/BibleFilesetConnectionsTable.py
# 3779 9/24
# 3802 9/24

python3 py/BibleFilesetCopyrightsTable.py
# 3728 9/24
# 3749 9/24

python3 py/BibleFilesetCopyrightOrganizationsTable.py
# 3728 9/24
# 3749 9/24

python3 py/BibleFilesetTagsTable.py
# 10704 9/24
# 10759 9/24

python3 py/AccessGroupFilesetsTable.py
# 3686 9/24
# 3707 9/24

# to be written possibly use info.json files
# python3 py/BibleBooksTable.py
# insert into valid_dbp.bible_books select * from dbp.bible_books where bible_id in (select id from valid_dbp.bibles);
# 42925 9/24
# 42991 9/24

python3 py/BibleFilesTable.py
# 1086357 9/24
# 1096274 9/24

python3 py/BibleFileTagsTable.py
# 0 9/24

python3 py/BibleFileVideoResolutionsTable.py
# 0 9/24

python3 py/BibleFileVideoTransportStreamTable.py
# 0 9/24













