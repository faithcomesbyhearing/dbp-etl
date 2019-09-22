#!/bin/sh -v -e


#must revised to run
#python3 py/BucketReader.py

python3 py/BiblesTable.py

python3 py/BibleTranslationsTable.py

python3 py/BibleFilesetsTable.py

python3 py/BibleFilesetConnectionsTable.py

python3 py/BibleFilesetCopyrightsTable.py

python3 py/BibleFilesetCopyrightOrganizationsTable.py

python3 py/BibleFilesetTagsTable.py

python3 py/AccessGroupFilesetsTable.py

# to be written possibly use info.json files
# python3 py/BibleBooksTable.py
# insert into valid_dbp.bible_books select * from dbp.bible_books where bible_id in (select id from valid_dbp.bibles);

python3 py/BibleFilesTable.py

python3 py/BibleFileTagsTable.py

python3 py/BibleFileVideoResolutionsTable.py

python3 py/BibleFileVideoTransportStreamTable.py













