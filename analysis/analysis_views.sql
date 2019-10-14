
use valid_dbp_views;

DROP VIEW IF EXISTS bibles_view;

CREATE VIEW bibles_view AS
SELECT bf.id AS fileset_id, b.id AS bible_id, b.language_id, b.versification, b.numeral_system_id, 
b.date, b.scope, b.script, b.derived, b.copyright, b.priority, b.reviewed, b.notes
FROM valid_dbp.bibles b, valid_dbp.bible_filesets bf, valid_dbp.bible_fileset_connections bfc
WHERE b.id = bfc.bible_id AND bfc.hash_id = bf.hash_id;

SELECT count(*) AS bibles FROM valid_dbp.bibles;
SELECT count(*) AS bibles_view FROM bibles_view;

DROP VIEW IF EXISTS bible_filesets_view;

CREATE VIEW bible_filesets_view AS
SELECT id AS fileset_id, hash_id, asset_id, set_type_code, set_size_code, hidden FROM valid_dbp.bible_filesets;

SELECT count(*) AS bible_filesets FROM valid_dbp.bible_filesets;
SELECT count(*) AS bible_filesets_view FROM bible_filesets_view;

DROP VIEW IF EXISTS bible_fileset_copyrights_view;

CREATE VIEW bible_fileset_copyrights_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id,
bfc.copyright_date, bfc.copyright, bfc.copyright_description, bfc.open_access
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_fileset_copyrights bfc
WHERE bf.hash_id = bfc.hash_id;

SELECT count(*) AS bible_fileset_copyrights FROM valid_dbp.bible_fileset_copyrights;
SELECT count(*) AS bible_fileset_copyrights_view FROM bible_fileset_copyrights_view;

DROP VIEW IF EXISTS bible_fileset_copyright_organizations_view;

CREATE VIEW bible_fileset_copyright_organizations_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id,
bfco.organization_id, bfco.organization_role
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_fileset_copyright_organizations bfco
where bf.hash_id = bfco.hash_id;

SELECT count(*) AS bible_fileset_copyright_organizations FROM valid_dbp.bible_fileset_copyright_organizations;
SELECT count(*) AS bible_fileset_copyright_organizations_view FROM bible_fileset_copyright_organizations_view;

DROP VIEW IF EXISTS bible_fileset_tags_view;

CREATE VIEW bible_fileset_tags_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id,
bft.name, bft.description, bft.admin_only, bft.notes, bft.iso, bft.language_id
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_fileset_tags bft
WHERE bf.hash_id = bft.hash_id;

SELECT count(*) AS bible_fileset_tags FROM valid_dbp.bible_fileset_tags;
SELECT count(*) AS bible_fileset_tags_view FROM bible_fileset_tags_view;

DROP VIEW IF EXISTS access_group_filesets_view;

CREATE VIEW access_group_filesets_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, agf.access_group_id
FROM valid_dbp.bible_filesets bf, valid_dbp.access_group_filesets agf
WHERE bf.hash_id = agf.hash_id;

SELECT count(*) AS access_group_filesets FROM valid_dbp.access_group_filesets;
SELECT count(*) AS access_group_filesets_view FROM access_group_filesets_view;

DROP VIEW IF EXISTS bible_files_view;

CREATE VIEW bible_files_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.book_id, bf2.chapter_start, bf2.chapter_end, bf2.verse_start, bf2.verse_end, 
bf2.file_name, bf2.file_size, bf2.duration
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_files bf2
WHERE bf.hash_id = bf2.hash_id;

SELECT count(*) AS bible_files FROM valid_dbp.bible_files;
SELECT count(*) AS bible_files_view FROM bible_files_view;

DROP VIEW IF EXISTS bible_file_tags_view;

CREATE VIEW bible_file_tags_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bft.file_id, bft.tag, bft.value, bft.admin_only
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_files bf2, valid_dbp.bible_file_tags bft
WHERE bf.hash_id = bf2.hash_id AND bft.file_id = bf2.id;

SELECT count(*) AS bible_file_tags FROM valid_dbp.bible_file_tags;
SELECT count(*) AS bible_file_tags_view FROM bible_file_tags_view;

DROP VIEW IF EXISTS bible_file_video_resolutions_view;

CREATE VIEW bible_file_video_resolutions_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.id AS file_id, bfvr.file_name, bfvr.bandwidth, bfvr.resolution_width, 
bfvr.resolution_height, bfvr.codec, bfvr.stream
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_files bf2, valid_dbp.bible_file_video_resolutions bfvr
WHERE bf.hash_id = bf2.hash_id AND bf2.id = bfvr.bible_file_id;

SELECT count(*) AS bible_file_video_resolutions FROM valid_dbp.bible_file_video_resolutions;
SELECT count(*) AS bible_file_video_resolutions_view FROM bible_file_video_resolutions_view;

DROP VIEW IF EXISTS bible_file_video_transport_stream_view;

CREATE VIEW bible_file_video_transport_stream_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.id AS file_id, bfvts.video_resolution_id, bfvts.file_name, bfvts.runtime
FROM valid_dbp.bible_filesets bf, valid_dbp.bible_files bf2, valid_dbp.bible_file_video_resolutions bfvr,
valid_dbp.bible_file_video_transport_stream bfvts
WHERE bf.hash_id = bf2.hash_id AND bf2.id = bfvr.bible_file_id AND bfvr.id = bfvts.video_resolution_id;

SELECT count(*) AS bible_file_video_transport_stream FROM valid_dbp.bible_file_video_transport_stream;
SELECT count(*) AS bible_file_video_transport_stream_view FROM bible_file_video_transport_stream_view;


