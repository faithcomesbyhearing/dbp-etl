


CREATE INDEX alphabet_numeral_systems_numeral_system_id_index ON alphabet_numeral_systems (numeral_system_id);
CREATE UNIQUE INDEX buckets_id_unique ON assets (id);
CREATE INDEX bible_books_bible_id_foreign ON bible_books (bible_id);
CREATE UNIQUE INDEX unique_bible_file_tag ON bible_file_tags (file_id,`tag`,`value`);
CREATE INDEX bible_fileset_connections_hash_id_foreign ON bible_fileset_connections (hash_id);
CREATE INDEX bible_fileset_tags_hash_id_index ON bible_fileset_tags (hash_id);
CREATE INDEX bible_filesets_id_index ON bible_filesets (id);
CREATE INDEX bible_filesets_hash_id_index ON bible_filesets (hash_id);
CREATE INDEX bible_text_hash_id_index ON bible_verses (hash_id);
CREATE UNIQUE INDEX bibles_id_unique ON bibles (id);
CREATE INDEX book_translations_language_id_foreign ON book_translations (language_id);
CREATE INDEX country_economy_country_id_foreign ON country_economy (country_id);
CREATE INDEX country_energy_country_id_foreign ON country_energy (country_id);
CREATE INDEX country_geography_country_id_foreign ON country_geography (country_id);
CREATE INDEX country_government_country_id_foreign ON country_government (country_id);
CREATE INDEX country_issues_country_id_foreign ON country_issues (country_id);
CREATE INDEX country_joshua_project_country_id_foreign ON country_joshua_project (country_id);
CREATE INDEX country_people_country_id_foreign ON country_people (country_id);
CREATE INDEX numeral_system_glyphs_numeral_system_id_index ON numeral_system_glyphs (numeral_system_id);
CREATE INDEX organization_logos_organization_id_foreign ON organization_logos (organization_id);
CREATE INDEX organization_relationships_organization_parent_id_foreign ON organization_relationships (organization_parent_id);
CREATE INDEX organization_translations_language_id_foreign ON organization_translations (language_id);
CREATE INDEX resource_translations_language_id_foreign ON resource_translations (language_id);


