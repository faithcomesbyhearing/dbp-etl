#!/bin/sh -v -e

#+----------+
#| asset_id |
#+----------+
#| dbp-prod |
#| dbp-vid  |
#| dbs-web  |
#------------

# First run table_delete.sh to remove bibles, filesets, files and related tables


mysql -uroot -p   <<END_SQL1

use dbp_only;

insert into dbp_only.bible_filesets
select * from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid');
-- 1699 deleted 9/24
-- 5823 inserted 9/25

insert into dbp_only.bibles
select * from dbp.bibles where id in 
(select bible_id from dbp.bible_fileset_connections where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid')));
-- 847 deleted 9/24
-- 1821 inserted 9/25

insert into dbp_only.bible_fileset_connections
select * from dbp.bible_fileset_connections where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 0 deleted 9/24
-- 5813 inserted 9/25

insert into dbp_only.bible_translations
select * from dbp.bible_translations where bible_id in 
(select bible_id from dbp.bible_fileset_connections where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid')));
-- 1601 deleted 9/24
-- 2680 inserted 9/25

insert into dbp_only.bible_books
select * from dbp.bible_books where bible_id in 
(select bible_id from dbp.bible_fileset_connections where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid')));
-- 25438 deleted 9/24
-- 55279 inserted 9/24

insert into dbp_only.bible_fileset_copyrights
select * from dbp.bible_fileset_copyrights where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 1699 deleted 9/24
-- 5812 inserted 9/25

insert into dbp_only.bible_fileset_copyright_organizations
select * from dbp.bible_fileset_copyright_organizations where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 1715 deleted 9/24
-- 6953 inserted 9/25

insert into dbp_only.bible_fileset_tags
select * from dbp.bible_fileset_tags where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 0 deleted 9/24
-- 14571 inserted 9/25

insert into dbp_only.access_group_filesets
select * from dbp.access_group_filesets where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 1699 deleted 9/24
-- 9361 inserted 9/25

insert into dbp_only.bible_files
select * from dbp.bible_files where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'));
-- 300312 deleted 9/24
-- 1153005 inserted 9/25

insert into dbp_only.bible_file_tags
select * from dbp.bible_file_tags where file_id in 
(select id from dbp.bible_files where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid')));
-- 0 deleted 9/24
-- 679056 inserted 9/25

insert into dbp_only.bible_file_video_resolutions
select * from dbp.bible_file_video_resolutions where bible_file_id in 
(select id from dbp.bible_files where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid')));
-- 0 deleted 9/24
-- 43934 inserted 9/25

insert into dbp_only.bible_file_video_transport_stream
select * from dbp.bible_file_video_transport_stream where video_resolution_id in 
(select id from dbp.bible_file_video_resolutions where bible_file_id in 
(select id from dbp.bible_files where hash_id in 
(select hash_id from dbp.bible_filesets where asset_id in ('dbp-prod', 'dbp-vid'))));
-- 0 deleted 9/24
-- 3186637 inserted 9/25

























END_SQL1






