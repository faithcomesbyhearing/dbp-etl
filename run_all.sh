#!/bin/sh -v -e

python3 py/BucketListingTable.py
# 1109357 9/24
# 1114097 9/24
# 1157283 9/27
# 30698082 9/27
# 1626574 10/1 verified
# 1624661 10/14 removed index.html and about.html files
# 1576432 10/14 changed from m3u8 to mp4
# 1559419 10/14 same, but eliminated 720 etc.
# 1419952 10/15 removed old type .html files, e.g. GN1.html

python3 py/BucketVerseSummaryTable.py
# 6543 10/11 verified
# 6509 10/14
# 6487 10/14
# 6483 10/15

python3 py/BibleFilesetsTable.py
# 3728 9/24
# 3749 9/24
# 6480 10/1
# 6484 10/3 verified count
# 6453 10/14
# 6431 10/15 after last 10/14 change

python3 py/BiblesTable.py
# 1397 9/24
# 1398 9/24
# 1797 10/1
# 2236 10/4 verified count
# 2209 10/14

python3 py/BibleFilesetConnectionsTable.py
# 3779 9/24
# 3802 9/24
# 6543 10/5 verified count
# 6509 10/14
# 6487 10/15

python3 py/BibleTranslationsTable.py
# 1397 9/24
# 1398 9/24
# 2236 10/4 not correct, multiples expected.
# 2209 10/14

python3 py/BibleFilesetCopyrightsTable.py
# 3728 9/24
# 3749 9/24
# 6484 10/5 verified count
# 6453 10/14
# 6431 10/15

python3 py/BibleFilesetCopyrightOrganizationsTable.py
# 3728 9/24
# 3749 9/24
# 6484 10/5 not correct, multiples expected.
# 6453 10/14
# 6431 10/15

python3 py/BibleFilesetTagsTable.py
# 10704 9/24
# 10759 9/24
# 15348 10/5 seems correct, but not proven
# 15338 10/14

python3 py/AccessGroupFilesetsTable.py
# 3686 9/24
# 3707 9/24
# 5349 10/6 better, not correct
# 5346 10/14

# to be written possibly use info.json files
# python3 py/BibleBooksTable.py
insert into valid_dbp.bible_books select * from dbp.bible_books where bible_id in (select id from valid_dbp.bibles);
# 42925 9/24
# 42991 9/24
# 70504 10/6 better
# 64490 10/9 ? I don't know why it changed.
# 64431 10/14

python3 py/BibleFilesTable.py
# 1086357 9/24
# 1096274 9/24
# 1397966 10/9
# 1369330 10/10 This is not correct, but move on
# 1369330 10/14

python3 py/BibleFileTagsTable.py
# 0 9/24
# 994257 10/12 in query, looks correct how to validate?

python3 py/BibleFileVideoResolutionsTable.py
# 0 9/24
# 49149 10/12 not verified

python3 py/BibleFileVideoTransportStreamTable.py
# 0 9/24













