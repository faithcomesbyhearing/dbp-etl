#!/bin/sh -v

mysql -uroot -p$MYSQL_PASSWD dbp << SQL_END1
-- tables with no foreign key reference
truncate table access_group_filesets;
truncate table bible_books;
truncate table bible_file_video_transport_stream;
truncate table bible_file_tags;
truncate table bible_fileset_connections;
truncate table bible_fileset_copyrights;
truncate table bible_fileset_copyright_organizations;
truncate table bible_fileset_tags;
truncate table bible_translations;

-- tables that are not updated by lptsmanager, that have no foreign key
-- each of the following tables has a foreign key to bibles, bible_files, or bible_filesets
truncate table bible_equivalents;
truncate table bible_file_timestamps;
truncate table bible_file_titles;
truncate table bible_links;
truncate table bible_organizations;
truncate table bible_translations;

set foreign_key_checks = 0;

-- not updated by lptsmanager, has foreign key to empty table bible_verse_concordance
truncate table bible_verses;

-- bible_file_video_resolutions has reference to bible_files, bible_file_video_transport_stream
truncate table bible_file_video_resolutions;

-- has foreign keys: bible_file_tags, bible_file_timestamps, bible_file_titles, bible_file_video_resolutions
truncate table bible_files;

-- has foreign keys: access_group_filesets, bible_files, bible_fileset_connections, 
-- bible_fileset_copyright_organizations, bible_fileset_copyrights, bible_fileset_relations, 
-- bible_fileset_tags, bible_verses
truncate table bible_filesets;

-- has foreign keys: bible_books, bible_equivalents, bible_fileset_connections, bible_links, 
-- bible_organizations, bible_translations, bible_translator (empty), videos (empty)
truncate table bibles;

set foreign_key_checks = 1;

SQL_END1

mysqldump -uroot -p$MYSQL_PASSWD  dbp > test_dbp.sql

