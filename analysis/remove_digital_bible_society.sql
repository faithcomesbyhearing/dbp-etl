#!/bin/sh -v -e

#+----------+
#| asset_id |
#+----------+
#| dbp-prod |
#| dbp-vid  |
#| dbs-web  |
#------------


mysql -uroot -p   <<END_SQL1

use dbp_only;

delete from bible_file_video_transport_stream where video_resolution_id in (select id from bible_file_video_resolutions where bible_file_id in (select id from bible_files where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web')));

delete from bible_file_video_resolutions where bible_file_id in (select id from bible_files where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web'));


delete from bible_file_tags where file_id in (select id from bible_files where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web'));

delete from bible_files where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');


delete from access_group_filesets where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');

delete from bible_fileset_tags where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');

delete from bible_fileset_copyright_organizations where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');

delete from bible_fileset_copyrights where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');



delete from bible_books where bible_id in (select bible_id from bible_fileset_connections where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web'));

delete from bible_translations where bible_id in (select bible_id from bible_fileset_connections where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web'));

delete from bibles where id in (select bible_id from bible_fileset_connections where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web'));

delete from bible_fileset_connections where hash_id in (select hash_id from bible_filesets where asset_id = 'dbs-web');

delete from bible_filesets where asset_id = 'dbs-web';

END_SQL1






