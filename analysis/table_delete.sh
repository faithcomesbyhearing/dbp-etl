#!/bin/sh -v -e


mysql -uroot -p   <<END_SQL1

use valid_dbp;

delete from bible_file_video_transport_stream;

delete from bible_file_video_resolutions;

delete from bible_file_tags;

delete from bible_files;

-- delete from bible_books;

delete from access_group_filesets;

delete from bible_fileset_tags;

delete from bible_fileset_copyright_organizations;

delete from bible_fileset_copyrights;

delete from bible_fileset_connections;

delete from bible_filesets;

delete from bible_translations;

delete from bibles;

END_SQL1