#!/bin/sh -v -e


mysql -uroot -p   <<END_SQL1

use valid_dbp;

select count(*) as bucket_listing from bucket_listing;

select count(*) as bibles from bibles;

select count(*) as bible_translations from bible_translations;

select count(*) as bible_filesets from bible_filesets;

select count(*) as bible_fileset_connections from bible_fileset_connections;

select count(*) as bible_fileset_copyrights from bible_fileset_copyrights;

select count(*) as bible_fileset_copyright_organizations from bible_fileset_copyright_organizations;

select count(*) as bible_fileset_tags from bible_fileset_tags;

select count(*) as access_group_filesets from access_group_filesets;

select count(*) as bible_books from bible_books;

select count(*) as bible_files from bible_files;

select count(*) as bible_file_tags from bible_file_tags;

select count(*) as bible_file_video_resolutions from bible_file_video_resolutions;

select count(*) as bible_file_video_transport_stream from bible_file_video_transport_stream;

END_SQL1