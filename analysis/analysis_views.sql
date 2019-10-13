
use dbp_view;

CREATE VIEW bible_filesets_view AS
SELECT id AS fileset_id, hash_id, asset_id, set_type_code, set_size_code, hidden FROM dbp.bible_filesets;

SELECT count(*) AS dbp.bible_filesets FROM bible_filesets;
SELECT count(*) AS bible_filesets_view FROM bible_filesets_view;

CREATE VIEW bible_fileset_copyrights_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id
bfc.copyright_date, bfc.copyright, bfc.copyright_description, bfc.open_access
FROM dbp.bible_filesets bf, dbp.bible_fileset_copyrights bfc
WHERE bf.hash_id = bfc.hash_id;

SELECT count(*) AS bible_fileset_copyrights FROM dbp.bible_fileset_copyrights;
SELECT count(*) AS bible_fileset_copyrights_view FROM bible_fileset_copyrights_view;

CREATE VIEW bible_fileset_copyright_organizations AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id,
bfco.organization_id, bfco.organization_role
FROM dbp.bible_filesets bf, dbp.bible_fileset_copyright_organizations bfco
where bf.hash_id = bfco.hash_id;

SELECT count(*) AS bible_fileset_copyright_organizations FROM dbp.bible_fileset_copyright_organizations;
SELECT count(*) AS bible_fileset_copyright_organizations_view FROM bible_fileset_copyright_organizations_view;

CREATE VIEW bible_fileset_tags_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id,
bft.name, bft.description, bft.admin_only, bft.notes, bft.iso, bft.language_id
FROM dbp.bible_filesets bf, dbp.bible_fileset_tags bft
WHERE bf.hash_id = bft.hash_id;

SELECT count(*) AS bible_fileset_tags FROM dbp.bible_fileset_tags;
SELECT count(*) AS bible_fileset_tags_view FROM bible_fileset_tags_view;

CREATE VIEW access_group_filesets_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, agf.access_group_id
FROM dbp.bible_filesets bf, dbp.access_group_filesets agf
WHERE bf.hash_id = agf.hash_id;

SELECT count(*) AS access_group_filesets FROM dbp.access_group_filesets;
SELECT count(*) AS access_group_filesets_view FROM access_group_filesets_view;

CREATE VIEW bible_files_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.book_id, bf2.chapter_start, bf2.chapter_end, bf2.verse_start, bf2.verse_end, 
bf2.file_name, bf2.file_size, bf2.duration
FROM dbp.bible_filesets bf, dbp.bible_files bf2
WHERE bf.hash_id = bf2.hash_id;

SELECT count(*) AS bible_files FROM dbp.bible_files;
SELECT count(*) AS bible_files_view FROM bible_files_view;

CREATE VIEW bible_file_tags_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bft.file_id, bft.tag, bft.value, bft.admin_only
FROM dbp.bible_filesets bf, dbp.bible_files bf2, dbp.bible_file_tags bft
WHERE bf.hash_id = bft.hash_id AND bft.file_id = bf2.id

SELECT count(*) AS bible_file_tags FROM dbp.bible_file_tags;
SELECT count(*) AS bible_file_tags_view FROM bible_file_tags_view;

CREATE VIEW bible_file_video_resolutions_view AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.id AS file_id, bfvr.file_name, bfvr.bandwidth, bfvr.resolution_width, 
bfvr.resolution_height, bfvr.codec, bfvr.stream
FROM dbp.bible_filesets bf, dbp.bible_files bf2, dbp.bible_file_video_resolutions bfvr
WHERE bf.hash_id = bf2.hash_id AND bf2.id = bfvr.file_id;

SELECT count(*) AS bible_file_video_resolutions FROM dbp.bible_file_video_resolutions;
SELECT count(*) AS bible_file_video_resolutions_view FROM bible_file_video_resolutions_view;

CREATE VIEW bible_file_video_transport_stream AS
SELECT bf.id AS fileset_id, bf.asset_id, bf.set_type_code, bf.hash_id, 
bf2.id AS file_id, bfvts.video_resolution_id, bfvts.file_name, bfvts.runtime
FROM dbp.bible_filesets bf, dbp.bible_files bf2, dbp.bible_file_video_resolutions bfvr,
dbp.bible_file_video_transport_stream bfvts
WHERE bf.hash_id = bf2.hash_id AND bf2.id = bfvr.file_id AND bfvr.id = bfts.video_resolution_id

SELECT count(*) AS bible_file_video_transport_stream FROM dbp.bible_file_video_transport_stream;
SELECT count(*) AS bible_file_video_transport_stream_view FROM bible_file_video_transport_stream_view;


