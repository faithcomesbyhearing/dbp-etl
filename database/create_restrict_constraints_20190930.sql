

ALTER TABLE access_group_filesets DROP FOREIGN KEY FK_access_groups_access_group_filesets;
ALTER TABLE access_group_filesets ADD CONSTRAINT FK_access_groups_access_group_filesets FOREIGN KEY (access_group_id) REFERENCES access_groups (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE access_group_filesets DROP FOREIGN KEY FK_bible_filesets_access_group_filesets;
ALTER TABLE access_group_filesets ADD CONSTRAINT FK_bible_filesets_access_group_filesets FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE access_group_types DROP FOREIGN KEY FK_access_groups_access_group_types;
ALTER TABLE access_group_types ADD CONSTRAINT FK_access_groups_access_group_types FOREIGN KEY (access_group_id) REFERENCES access_groups (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE access_group_types DROP FOREIGN KEY FK_access_types_access_group_types;
ALTER TABLE access_group_types ADD CONSTRAINT FK_access_types_access_group_types FOREIGN KEY (access_type_id) REFERENCES access_types (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE access_types DROP FOREIGN KEY FK_countries_access_types;
ALTER TABLE access_types ADD CONSTRAINT FK_countries_access_types FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE alphabet_fonts DROP FOREIGN KEY FK_alphabets_alphabet_fonts;
ALTER TABLE alphabet_fonts ADD CONSTRAINT FK_alphabets_alphabet_fonts FOREIGN KEY (script_id) REFERENCES alphabets (script) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE alphabet_language DROP FOREIGN KEY FK_alphabets_alphabet_language;
ALTER TABLE alphabet_language ADD CONSTRAINT FK_alphabets_alphabet_language FOREIGN KEY (script_id) REFERENCES alphabets (script) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE alphabet_language DROP FOREIGN KEY FK_languages_alphabet_language;
ALTER TABLE alphabet_language ADD CONSTRAINT FK_languages_alphabet_language FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE alphabet_numeral_systems DROP FOREIGN KEY FK_alphabets_alphabet_numeral_systems;
ALTER TABLE alphabet_numeral_systems ADD CONSTRAINT FK_alphabets_alphabet_numeral_systems FOREIGN KEY (script_id) REFERENCES alphabets (script) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE assets DROP FOREIGN KEY FK_organizations_assets;
ALTER TABLE assets ADD CONSTRAINT FK_organizations_assets FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_books DROP FOREIGN KEY FK_bibles_bible_books;
ALTER TABLE bible_books ADD CONSTRAINT FK_bibles_bible_books FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_books DROP FOREIGN KEY FK_books_bible_books;
ALTER TABLE bible_books ADD CONSTRAINT FK_books_bible_books FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_equivalents DROP FOREIGN KEY FK_bibles_bible_equivalents;
ALTER TABLE bible_equivalents ADD CONSTRAINT FK_bibles_bible_equivalents FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_equivalents DROP FOREIGN KEY FK_organizations_bible_equivalents;
ALTER TABLE bible_equivalents ADD CONSTRAINT FK_organizations_bible_equivalents FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_file_tags DROP FOREIGN KEY FK_bible_files_bible_file_tags;
ALTER TABLE bible_file_tags ADD CONSTRAINT FK_bible_files_bible_file_tags FOREIGN KEY (file_id) REFERENCES bible_files (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_file_timestamps DROP FOREIGN KEY FK_bible_files_bible_file_timestamps;
ALTER TABLE bible_file_timestamps ADD CONSTRAINT FK_bible_files_bible_file_timestamps FOREIGN KEY (bible_file_id) REFERENCES bible_files (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_file_titles DROP FOREIGN KEY FK_bible_files_bible_file_titles;
ALTER TABLE bible_file_titles ADD CONSTRAINT FK_bible_files_bible_file_titles FOREIGN KEY (file_id) REFERENCES bible_files (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_file_titles DROP FOREIGN KEY FK_languages_bible_file_titles;
ALTER TABLE bible_file_titles ADD CONSTRAINT FK_languages_bible_file_titles FOREIGN KEY (iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_file_video_resolutions DROP FOREIGN KEY FK_bible_files_bible_file_video_resolutions;
ALTER TABLE bible_file_video_resolutions ADD CONSTRAINT FK_bible_files_bible_file_video_resolutions FOREIGN KEY (bible_file_id) REFERENCES bible_files (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_file_video_transport_stream DROP FOREIGN KEY FK_bible_file_video_resolutions_bible_file_video_transport_strea;
ALTER TABLE bible_file_video_transport_stream ADD CONSTRAINT FK_bible_file_video_resolutions_bible_file_video_transport_strea FOREIGN KEY (video_resolution_id) REFERENCES bible_file_video_resolutions (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_files DROP FOREIGN KEY FK_bible_filesets_bible_files;
ALTER TABLE bible_files ADD CONSTRAINT FK_bible_filesets_bible_files FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_files DROP FOREIGN KEY FK_books_bible_files;
ALTER TABLE bible_files ADD CONSTRAINT FK_books_bible_files FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_fileset_connections DROP FOREIGN KEY FK_bibles_bible_fileset_connections;
ALTER TABLE bible_fileset_connections ADD CONSTRAINT FK_bibles_bible_fileset_connections FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_connections DROP FOREIGN KEY FK_bible_filesets_bible_fileset_connections;
ALTER TABLE bible_fileset_connections ADD CONSTRAINT FK_bible_filesets_bible_fileset_connections FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_fileset_copyright_organizations DROP FOREIGN KEY FK_bible_filesets_bible_fileset_copyright_organizations;
ALTER TABLE bible_fileset_copyright_organizations ADD CONSTRAINT FK_bible_filesets_bible_fileset_copyright_organizations FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_copyright_organizations DROP FOREIGN KEY FK_bible_fileset_copyright_roles_bible_fileset_copyright_organiz;
ALTER TABLE bible_fileset_copyright_organizations ADD CONSTRAINT FK_bible_fileset_copyright_roles_bible_fileset_copyright_organiz FOREIGN KEY (organization_role) REFERENCES bible_fileset_copyright_roles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_copyright_organizations DROP FOREIGN KEY FK_organizations_bible_fileset_copyright_organizations;
ALTER TABLE bible_fileset_copyright_organizations ADD CONSTRAINT FK_organizations_bible_fileset_copyright_organizations FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_fileset_copyrights DROP FOREIGN KEY FK_bible_filesets_bible_fileset_copyrights;
ALTER TABLE bible_fileset_copyrights ADD CONSTRAINT FK_bible_filesets_bible_fileset_copyrights FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_fileset_relations DROP FOREIGN KEY FK_bible_filesets_bible_fileset_relations_child_hash_id;
ALTER TABLE bible_fileset_relations ADD CONSTRAINT FK_bible_filesets_bible_fileset_relations_child_hash_id FOREIGN KEY (child_hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_relations DROP FOREIGN KEY FK_bible_filesets_bible_fileset_relations_parent_hash_id;
ALTER TABLE bible_fileset_relations ADD CONSTRAINT FK_bible_filesets_bible_fileset_relations_parent_hash_id FOREIGN KEY (parent_hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_fileset_tags DROP FOREIGN KEY FK_bible_filesets_bible_fileset_tags;
ALTER TABLE bible_fileset_tags ADD CONSTRAINT FK_bible_filesets_bible_fileset_tags FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_tags DROP FOREIGN KEY FK_languages_bible_fileset_tags;
ALTER TABLE bible_fileset_tags ADD CONSTRAINT FK_languages_bible_fileset_tags FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_fileset_tags DROP FOREIGN KEY FK_languages_bible_fileset_tags_iso;
ALTER TABLE bible_fileset_tags ADD CONSTRAINT FK_languages_bible_fileset_tags_iso FOREIGN KEY (iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_filesets DROP FOREIGN KEY FK_assets_bible_filesets;
ALTER TABLE bible_filesets ADD CONSTRAINT FK_assets_bible_filesets FOREIGN KEY (asset_id) REFERENCES assets (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_filesets DROP FOREIGN KEY FK_bible_fileset_sizes_bible_filesets;
ALTER TABLE bible_filesets ADD CONSTRAINT FK_bible_fileset_sizes_bible_filesets FOREIGN KEY (set_size_code) REFERENCES bible_fileset_sizes (set_size_code) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_filesets DROP FOREIGN KEY FK_bible_fileset_types_bible_filesets;
ALTER TABLE bible_filesets ADD CONSTRAINT FK_bible_fileset_types_bible_filesets FOREIGN KEY (set_type_code) REFERENCES bible_fileset_types (set_type_code) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_links DROP FOREIGN KEY FK_bibles_bible_links;
ALTER TABLE bible_links ADD CONSTRAINT FK_bibles_bible_links FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_links DROP FOREIGN KEY FK_organizations_bible_links;
ALTER TABLE bible_links ADD CONSTRAINT FK_organizations_bible_links FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_organizations DROP FOREIGN KEY FK_bibles_bible_organizations;
ALTER TABLE bible_organizations ADD CONSTRAINT FK_bibles_bible_organizations FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_organizations DROP FOREIGN KEY FK_organizations_bible_organizations;
ALTER TABLE bible_organizations ADD CONSTRAINT FK_organizations_bible_organizations FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_size_translations DROP FOREIGN KEY FK_bible_fileset_sizes_bible_size_translations;
ALTER TABLE bible_size_translations ADD CONSTRAINT FK_bible_fileset_sizes_bible_size_translations FOREIGN KEY (set_size_code) REFERENCES bible_fileset_sizes (set_size_code) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_size_translations DROP FOREIGN KEY FK_languages_bible_size_translations;
ALTER TABLE bible_size_translations ADD CONSTRAINT FK_languages_bible_size_translations FOREIGN KEY (iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_translations DROP FOREIGN KEY FK_bibles_bible_translations;
ALTER TABLE bible_translations ADD CONSTRAINT FK_bibles_bible_translations FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_translations DROP FOREIGN KEY FK_languages_bible_translations;
ALTER TABLE bible_translations ADD CONSTRAINT FK_languages_bible_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_translator DROP FOREIGN KEY FK_bibles_bible_translator;
ALTER TABLE bible_translator ADD CONSTRAINT FK_bibles_bible_translator FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_translator DROP FOREIGN KEY FK_translators_bible_translator;
ALTER TABLE bible_translator ADD CONSTRAINT FK_translators_bible_translator FOREIGN KEY (translator_id) REFERENCES translators (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_verse_concordance DROP FOREIGN KEY FK_bible_concordance_bible_verse_concordance;
ALTER TABLE bible_verse_concordance ADD CONSTRAINT FK_bible_concordance_bible_verse_concordance FOREIGN KEY (bible_concordance) REFERENCES bible_concordance (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bible_verse_concordance DROP FOREIGN KEY FK_bible_verses_bible_verse_concordance;
ALTER TABLE bible_verse_concordance ADD CONSTRAINT FK_bible_verses_bible_verse_concordance FOREIGN KEY (bible_verse_id) REFERENCES bible_verses (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bible_verses DROP FOREIGN KEY bible_text_hash_id_foreign;
ALTER TABLE bible_verses ADD CONSTRAINT bible_text_hash_id_foreign FOREIGN KEY (hash_id) REFERENCES bible_filesets (hash_id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE bibles DROP FOREIGN KEY FK_alphabets_bibles;
ALTER TABLE bibles ADD CONSTRAINT FK_alphabets_bibles FOREIGN KEY (script) REFERENCES alphabets (script) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bibles DROP FOREIGN KEY FK_languages_bibles;
ALTER TABLE bibles ADD CONSTRAINT FK_languages_bibles FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE bibles DROP FOREIGN KEY FK_numeral_systems_bibles;
ALTER TABLE bibles ADD CONSTRAINT FK_numeral_systems_bibles FOREIGN KEY (numeral_system_id) REFERENCES numeral_systems (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE book_translations DROP FOREIGN KEY FK_books_book_translations;
ALTER TABLE book_translations ADD CONSTRAINT FK_books_book_translations FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE book_translations DROP FOREIGN KEY FK_languages_book_translations;
ALTER TABLE book_translations ADD CONSTRAINT FK_languages_book_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE commentary_sections DROP FOREIGN KEY FK_books_commentary_sections;
ALTER TABLE commentary_sections ADD CONSTRAINT FK_books_commentary_sections FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE commentary_sections DROP FOREIGN KEY FK_commentaries_commentary_sections;
ALTER TABLE commentary_sections ADD CONSTRAINT FK_commentaries_commentary_sections FOREIGN KEY (commentary_id) REFERENCES commentaries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE commentary_translations DROP FOREIGN KEY FK_commentaries_commentary_translations;
ALTER TABLE commentary_translations ADD CONSTRAINT FK_commentaries_commentary_translations FOREIGN KEY (commentary_id) REFERENCES commentaries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE commentary_translations DROP FOREIGN KEY FK_languages_commentary_translations;
ALTER TABLE commentary_translations ADD CONSTRAINT FK_languages_commentary_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE connection_translations DROP FOREIGN KEY FK_languages_connection_translations;
ALTER TABLE connection_translations ADD CONSTRAINT FK_languages_connection_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE connection_translations DROP FOREIGN KEY FK_resources_connection_translations;
ALTER TABLE connection_translations ADD CONSTRAINT FK_resources_connection_translations FOREIGN KEY (resource_id) REFERENCES resources (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE connections DROP FOREIGN KEY FK_organizations_connections;
ALTER TABLE connections ADD CONSTRAINT FK_organizations_connections FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_communications DROP FOREIGN KEY FK_countries_country_communications;
ALTER TABLE country_communications ADD CONSTRAINT FK_countries_country_communications FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_economy DROP FOREIGN KEY FK_countries_country_economy;
ALTER TABLE country_economy ADD CONSTRAINT FK_countries_country_economy FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_energy DROP FOREIGN KEY FK_countries_country_energy;
ALTER TABLE country_energy ADD CONSTRAINT FK_countries_country_energy FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_geography DROP FOREIGN KEY FK_countries_country_geography;
ALTER TABLE country_geography ADD CONSTRAINT FK_countries_country_geography FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_government DROP FOREIGN KEY FK_countries_country_government;
ALTER TABLE country_government ADD CONSTRAINT FK_countries_country_government FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_issues DROP FOREIGN KEY FK_countries_country_issues;
ALTER TABLE country_issues ADD CONSTRAINT FK_countries_country_issues FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_joshua_project DROP FOREIGN KEY FK_countries_country_joshua_project;
ALTER TABLE country_joshua_project ADD CONSTRAINT FK_countries_country_joshua_project FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE country_joshua_project DROP FOREIGN KEY FK_languages_country_joshua_project;
ALTER TABLE country_joshua_project ADD CONSTRAINT FK_languages_country_joshua_project FOREIGN KEY (language_official_iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_language DROP FOREIGN KEY FK_countries_country_language;
ALTER TABLE country_language ADD CONSTRAINT FK_countries_country_language FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE country_language DROP FOREIGN KEY FK_languages_country_language;
ALTER TABLE country_language ADD CONSTRAINT FK_languages_country_language FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_maps DROP FOREIGN KEY FK_countries_country_maps;
ALTER TABLE country_maps ADD CONSTRAINT FK_countries_country_maps FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_people DROP FOREIGN KEY FK_countries_country_people;
ALTER TABLE country_people ADD CONSTRAINT FK_countries_country_people FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_people_ethnicities DROP FOREIGN KEY FK_countries_country_people_ethnicities;
ALTER TABLE country_people_ethnicities ADD CONSTRAINT FK_countries_country_people_ethnicities FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_regions DROP FOREIGN KEY FK_countries_country_regions;
ALTER TABLE country_regions ADD CONSTRAINT FK_countries_country_regions FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE country_regions DROP FOREIGN KEY FK_languages_country_regions;
ALTER TABLE country_regions ADD CONSTRAINT FK_languages_country_regions FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_religions DROP FOREIGN KEY FK_countries_country_religions;
ALTER TABLE country_religions ADD CONSTRAINT FK_countries_country_religions FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_translations DROP FOREIGN KEY FK_countries_country_translations;
ALTER TABLE country_translations ADD CONSTRAINT FK_countries_country_translations FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE country_translations DROP FOREIGN KEY FK_languages_country_translations;
ALTER TABLE country_translations ADD CONSTRAINT FK_languages_country_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE country_transportation DROP FOREIGN KEY FK_countries_country_transportation;
ALTER TABLE country_transportation ADD CONSTRAINT FK_countries_country_transportation FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE language_bibleInfo DROP FOREIGN KEY FK_languages_language_bibleInfo;
ALTER TABLE language_bibleInfo ADD CONSTRAINT FK_languages_language_bibleInfo FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE language_classifications DROP FOREIGN KEY FK_languages_language_classifications;
ALTER TABLE language_classifications ADD CONSTRAINT FK_languages_language_classifications FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE language_codes DROP FOREIGN KEY FK_languages_language_codes;
ALTER TABLE language_codes ADD CONSTRAINT FK_languages_language_codes FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE language_dialects DROP FOREIGN KEY FK_languages_language_dialects;
ALTER TABLE language_dialects ADD CONSTRAINT FK_languages_language_dialects FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE language_translations DROP FOREIGN KEY FK_languages_language_translations_language_source_id;
ALTER TABLE language_translations ADD CONSTRAINT FK_languages_language_translations_language_source_id FOREIGN KEY (language_source_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE language_translations DROP FOREIGN KEY FK_languages_language_translations_language_tranlation_id;
ALTER TABLE language_translations ADD CONSTRAINT FK_languages_language_translations_language_tranlation_id FOREIGN KEY (language_translation_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE languages DROP FOREIGN KEY FK_countries_languages;
ALTER TABLE languages ADD CONSTRAINT FK_countries_languages FOREIGN KEY (country_id) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE languages DROP FOREIGN KEY FK_language_status_languages;
ALTER TABLE languages ADD CONSTRAINT FK_language_status_languages FOREIGN KEY (status_id) REFERENCES language_status (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE numeral_system_glyphs DROP FOREIGN KEY FK_numeral_systems_numeral_system_glyphs;
ALTER TABLE numeral_system_glyphs ADD CONSTRAINT FK_numeral_systems_numeral_system_glyphs FOREIGN KEY (numeral_system_id) REFERENCES numeral_systems (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE organization_logos DROP FOREIGN KEY FK_languages_organization_logos_language_id;
ALTER TABLE organization_logos ADD CONSTRAINT FK_languages_organization_logos_language_id FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE organization_logos DROP FOREIGN KEY FK_languages_organization_logos_language_iso;
ALTER TABLE organization_logos ADD CONSTRAINT FK_languages_organization_logos_language_iso FOREIGN KEY (language_iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE organization_logos DROP FOREIGN KEY FK_organizations_organization_logos;
ALTER TABLE organization_logos ADD CONSTRAINT FK_organizations_organization_logos FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE organization_relationships DROP FOREIGN KEY FK_organizations_organization_relationships_organization_child;
ALTER TABLE organization_relationships ADD CONSTRAINT FK_organizations_organization_relationships_organization_child FOREIGN KEY (organization_child_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE organization_relationships DROP FOREIGN KEY FK_organizations_organization_relationships_organization_parent;
ALTER TABLE organization_relationships ADD CONSTRAINT FK_organizations_organization_relationships_organization_parent FOREIGN KEY (organization_parent_id) REFERENCES organizations (id)  ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE organization_translations DROP FOREIGN KEY FK_languages_organization_translations;
ALTER TABLE organization_translations ADD CONSTRAINT FK_languages_organization_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE organization_translations DROP FOREIGN KEY FK_organizations_organization_translations;
ALTER TABLE organization_translations ADD CONSTRAINT FK_organizations_organization_translations FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE organizations DROP FOREIGN KEY FK_countries_organizations;
ALTER TABLE organizations ADD CONSTRAINT FK_countries_organizations FOREIGN KEY (country) REFERENCES countries (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE resource_links DROP FOREIGN KEY FK_resources_resource_links;
ALTER TABLE resource_links ADD CONSTRAINT FK_resources_resource_links FOREIGN KEY (resource_id) REFERENCES resources (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE resource_translations DROP FOREIGN KEY FK_languages_resource_translations;
ALTER TABLE resource_translations ADD CONSTRAINT FK_languages_resource_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE resource_translations DROP FOREIGN KEY FK_resources_resource_translations;
ALTER TABLE resource_translations ADD CONSTRAINT FK_resources_resource_translations FOREIGN KEY (resource_id) REFERENCES resources (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE resources DROP FOREIGN KEY FK_languages_resources_iso;
ALTER TABLE resources ADD CONSTRAINT FK_languages_resources_iso FOREIGN KEY (iso) REFERENCES languages (iso) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE resources DROP FOREIGN KEY FK_languages_resources_language_id;
ALTER TABLE resources ADD CONSTRAINT FK_languages_resources_language_id FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE resources DROP FOREIGN KEY FK_organizations_resources;
ALTER TABLE resources ADD CONSTRAINT FK_organizations_resources FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE translator_relations DROP FOREIGN KEY FK_organizations_translator_relations;
ALTER TABLE translator_relations ADD CONSTRAINT FK_organizations_translator_relations FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE translator_relations DROP FOREIGN KEY FK_translaors_translator_relations_translator_relation_id;
ALTER TABLE translator_relations ADD CONSTRAINT FK_translaors_translator_relations_translator_relation_id FOREIGN KEY (translator_relation_id) REFERENCES translators (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE translator_relations DROP FOREIGN KEY FK_translators_translator_relations;
ALTER TABLE translator_relations ADD CONSTRAINT FK_translators_translator_relations FOREIGN KEY (translator_id) REFERENCES translators (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE video_sources DROP FOREIGN KEY FK_videos_video_sources;
ALTER TABLE video_sources ADD CONSTRAINT FK_videos_video_sources FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE video_tags DROP FOREIGN KEY FK_books_video_tags;
ALTER TABLE video_tags ADD CONSTRAINT FK_books_video_tags FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE video_tags DROP FOREIGN KEY FK_languages_video_tags;
ALTER TABLE video_tags ADD CONSTRAINT FK_languages_video_tags FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE video_tags DROP FOREIGN KEY FK_organizations_video_tags;
ALTER TABLE video_tags ADD CONSTRAINT FK_organizations_video_tags FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE video_tags DROP FOREIGN KEY FK_videos_video_tags;
ALTER TABLE video_tags ADD CONSTRAINT FK_videos_video_tags FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE video_translations DROP FOREIGN KEY FK_languages_video_translations;
ALTER TABLE video_translations ADD CONSTRAINT FK_languages_video_translations FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE video_translations DROP FOREIGN KEY FK_videos_video_translations;
ALTER TABLE video_translations ADD CONSTRAINT FK_videos_video_translations FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

ALTER TABLE videos DROP FOREIGN KEY FK_bibles_videos;
ALTER TABLE videos ADD CONSTRAINT FK_bibles_videos FOREIGN KEY (bible_id) REFERENCES bibles (id) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE videos DROP FOREIGN KEY FK_languages_videos;
ALTER TABLE videos ADD CONSTRAINT FK_languages_videos FOREIGN KEY (language_id) REFERENCES languages (id) ON DELETE RESTRICT ON UPDATE RESTRICT;

