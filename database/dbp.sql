-- MySQL dump 10.14  Distrib 5.5.62-MariaDB, for Linux (x86_64)
--
-- Host: dev1.cp6dghsmdxd5.us-west-2.rds.amazonaws.com    Database: dev_190614
-- ------------------------------------------------------
-- Server version	5.6.10

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `access_group_filesets`
--

DROP TABLE IF EXISTS `access_group_filesets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `access_group_filesets` (
  `access_group_id` int(10) unsigned NOT NULL,
  `hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`access_group_id`,`hash_id`),
  KEY `FK_access_group_filesets__hash_id` (`hash_id`),
  CONSTRAINT `FK_access_groups_access_group_filesets` FOREIGN KEY (`access_group_id`) REFERENCES `access_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_filesets_access_group_filesets` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `access_group_types`
--

DROP TABLE IF EXISTS `access_group_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `access_group_types` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `access_group_id` int(10) unsigned NOT NULL,
  `access_type_id` int(10) unsigned NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `access_group_types_access_group_id_foreign` (`access_group_id`),
  KEY `access_group_types_access_type_id_foreign` (`access_type_id`),
  CONSTRAINT `FK_access_groups_access_group_types` FOREIGN KEY (`access_group_id`) REFERENCES `access_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_access_types_access_group_types` FOREIGN KEY (`access_type_id`) REFERENCES `access_types` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `access_groups`
--

DROP TABLE IF EXISTS `access_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `access_groups` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `access_types`
--

DROP TABLE IF EXISTS `access_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `access_types` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(24) COLLATE utf8mb4_unicode_ci NOT NULL,
  `country_id` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `continent_id` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `allowed` tinyint(1) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `access_types_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_access_types` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `alphabet_fonts`
--

DROP TABLE IF EXISTS `alphabet_fonts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alphabet_fonts` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `script_id` char(4) COLLATE utf8mb4_unicode_ci NOT NULL,
  `font_name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `font_filename` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `font_weight` int(10) unsigned DEFAULT NULL,
  `copyright` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `italic` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `alphabet_fonts_script_id_foreign` (`script_id`),
  CONSTRAINT `FK_alphabets_alphabet_fonts` FOREIGN KEY (`script_id`) REFERENCES `alphabets` (`script`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `alphabet_language`
--

DROP TABLE IF EXISTS `alphabet_language`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alphabet_language` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `script_id` char(4) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `alphabet_language_language_id_foreign` (`language_id`),
  KEY `alphabet_language_script_id_index` (`script_id`),
  CONSTRAINT `FK_alphabets_alphabet_language` FOREIGN KEY (`script_id`) REFERENCES `alphabets` (`script`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_alphabet_language` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=327 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `alphabet_numeral_systems`
--

DROP TABLE IF EXISTS `alphabet_numeral_systems`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alphabet_numeral_systems` (
  `numeral_system_id` char(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `script_id` char(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`numeral_system_id`),
  KEY `alphabet_numeral_systems_script_id_foreign` (`script_id`),
  KEY `alphabet_numeral_systems_numeral_system_id_index` (`numeral_system_id`),
  CONSTRAINT `FK_alphabets_alphabet_numeral_systems` FOREIGN KEY (`script_id`) REFERENCES `alphabets` (`script`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `alphabets`
--

DROP TABLE IF EXISTS `alphabets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alphabets` (
  `script` char(4) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `unicode_pdf` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `family` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `white_space` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `open_type_tag` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `complex_positioning` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `requires_font` tinyint(1) NOT NULL DEFAULT '0',
  `unicode` tinyint(1) NOT NULL DEFAULT '1',
  `diacritics` tinyint(1) DEFAULT NULL,
  `contextual_forms` tinyint(1) DEFAULT NULL,
  `reordering` tinyint(1) DEFAULT NULL,
  `case` tinyint(1) DEFAULT NULL,
  `split_graphs` tinyint(1) DEFAULT NULL,
  `status` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `baseline` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ligatures` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `direction` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `direction_notes` text COLLATE utf8mb4_unicode_ci,
  `sample` text COLLATE utf8mb4_unicode_ci,
  `sample_img` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`script`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `assets`
--

DROP TABLE IF EXISTS `assets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `assets` (
  `id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `organization_id` int(10) unsigned NOT NULL,
  `asset_type` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `hidden` tinyint(1) NOT NULL DEFAULT '0',
  `base_name` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `buckets_id_unique` (`id`),
  KEY `buckets_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_organizations_assets` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_books`
--

DROP TABLE IF EXISTS `bible_books`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_books` (
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_short` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chapters` varchar(491) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`bible_id`,`book_id`),
  KEY `bible_books_bible_id_foreign` (`bible_id`),
  KEY `bible_books_book_id_foreign` (`book_id`),
  CONSTRAINT `FK_bibles_bible_books` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_books_bible_books` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_concordance`
--

DROP TABLE IF EXISTS `bible_concordance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_concordance` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `key_word` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `bible_concordance_key_word_unique` (`key_word`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_equivalents`
--

DROP TABLE IF EXISTS `bible_equivalents`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_equivalents` (
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `equivalent_id` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `organization_id` int(10) unsigned NOT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `site` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `suffix` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `bible_equivalents_bible_id_foreign` (`bible_id`),
  KEY `bible_equivalents_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_bibles_bible_equivalents` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_bible_equivalents` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_file_tags`
--

DROP TABLE IF EXISTS `bible_file_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_file_tags` (
  `file_id` int(10) unsigned NOT NULL,
  `tag` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `value` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `admin_only` tinyint(1) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`file_id`,`tag`),
  UNIQUE KEY `unique_bible_file_tag` (`file_id`,`tag`,`value`),
  CONSTRAINT `FK_bible_files_bible_file_tags` FOREIGN KEY (`file_id`) REFERENCES `bible_files` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_file_timestamps`
--

DROP TABLE IF EXISTS `bible_file_timestamps`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_file_timestamps` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `bible_file_id` int(10) unsigned NOT NULL,
  `verse_start` tinyint(3) unsigned DEFAULT NULL,
  `verse_end` tinyint(3) unsigned DEFAULT NULL,
  `timestamp` double(8,2) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `bible_file_timestamps_file_id_foreign` (`bible_file_id`),
  CONSTRAINT `FK_bible_files_bible_file_timestamps` FOREIGN KEY (`bible_file_id`) REFERENCES `bible_files` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=39134 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_file_titles`
--

DROP TABLE IF EXISTS `bible_file_titles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_file_titles` (
  `file_id` int(10) unsigned NOT NULL,
  `iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `key_words` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `bible_file_titles_file_id_foreign` (`file_id`),
  KEY `bible_file_titles_iso_foreign` (`iso`),
  CONSTRAINT `FK_bible_files_bible_file_titles` FOREIGN KEY (`file_id`) REFERENCES `bible_files` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_bible_file_titles` FOREIGN KEY (`iso`) REFERENCES `languages` (`iso`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_file_video_resolutions`
--

DROP TABLE IF EXISTS `bible_file_video_resolutions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_file_video_resolutions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `bible_file_id` int(10) unsigned NOT NULL,
  `file_name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `bandwidth` int(10) unsigned NOT NULL,
  `resolution_width` int(10) unsigned NOT NULL,
  `resolution_height` int(10) unsigned NOT NULL,
  `codec` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `stream` tinyint(1) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_name` (`file_name`),
  KEY `bible_file_video_resolutions_bible_file_id_foreign` (`bible_file_id`),
  CONSTRAINT `FK_bible_files_bible_file_video_resolutions` FOREIGN KEY (`bible_file_id`) REFERENCES `bible_files` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=29611 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_file_video_transport_stream`
--

DROP TABLE IF EXISTS `bible_file_video_transport_stream`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_file_video_transport_stream` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `video_resolution_id` int(10) unsigned NOT NULL,
  `file_name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `runtime` double(8,2) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_name` (`file_name`),
  KEY `bible_file_video_transport_stream_video_resolution_id_foreign` (`video_resolution_id`),
  CONSTRAINT `FK_bible_file_video_resolutions_bible_file_video_transport_strea` FOREIGN KEY (`video_resolution_id`) REFERENCES `bible_file_video_resolutions` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2120668 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_files`
--

DROP TABLE IF EXISTS `bible_files`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_files` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hash_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chapter_start` tinyint(3) unsigned DEFAULT NULL,
  `chapter_end` tinyint(3) unsigned DEFAULT NULL,
  `verse_start` tinyint(3) unsigned DEFAULT NULL,
  `verse_end` tinyint(3) unsigned DEFAULT NULL,
  `file_name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `file_size` int(10) unsigned DEFAULT NULL,
  `duration` int(10) unsigned DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_bible_file_by_reference` (`hash_id`,`book_id`,`chapter_start`,`verse_start`),
  KEY `bible_files_book_id_foreign` (`book_id`),
  CONSTRAINT `FK_bible_filesets_bible_files` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_books_bible_files` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1928370 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_connections`
--

DROP TABLE IF EXISTS `bible_fileset_connections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_connections` (
  `hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`hash_id`,`bible_id`),
  KEY `bible_fileset_connections_hash_id_foreign` (`hash_id`),
  KEY `bible_fileset_connections_bible_id_index` (`bible_id`),
  CONSTRAINT `FK_bibles_bible_fileset_connections` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_filesets_bible_fileset_connections` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_copyright_organizations`
--

DROP TABLE IF EXISTS `bible_fileset_copyright_organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_copyright_organizations` (
  `hash_id` char(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `organization_id` int(10) unsigned NOT NULL,
  `organization_role` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`hash_id`,`organization_id`),
  KEY `FK_org_id` (`organization_id`),
  KEY `FK_org_role` (`organization_role`),
  CONSTRAINT `FK_bible_filesets_bible_fileset_copyright_organizations` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_fileset_copyright_roles_bible_fileset_copyright_organiz` FOREIGN KEY (`organization_role`) REFERENCES `bible_fileset_copyright_roles` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_bible_fileset_copyright_organizations` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_copyright_roles`
--

DROP TABLE IF EXISTS `bible_fileset_copyright_roles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_copyright_roles` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_copyrights`
--

DROP TABLE IF EXISTS `bible_fileset_copyrights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_copyrights` (
  `hash_id` char(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `copyright_date` varchar(191) CHARACTER SET utf8mb4 DEFAULT NULL,
  `copyright` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `copyright_description` text CHARACTER SET utf8mb4 NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `open_access` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`hash_id`),
  CONSTRAINT `FK_bible_filesets_bible_fileset_copyrights` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_relations`
--

DROP TABLE IF EXISTS `bible_fileset_relations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_relations` (
  `id` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `parent_hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `child_hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `relationship` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `bible_fileset_relations_parent_hash_id_index` (`parent_hash_id`),
  KEY `bible_fileset_relations_child_hash_id_index` (`child_hash_id`),
  CONSTRAINT `FK_bible_filesets_bible_fileset_relations_child_hash_id` FOREIGN KEY (`child_hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_filesets_bible_fileset_relations_parent_hash_id` FOREIGN KEY (`parent_hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_sizes`
--

DROP TABLE IF EXISTS `bible_fileset_sizes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_sizes` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `set_size_code` char(9) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `bible_fileset_sizes_set_size_code_unique` (`set_size_code`),
  UNIQUE KEY `bible_fileset_sizes_name_unique` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_tags`
--

DROP TABLE IF EXISTS `bible_fileset_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_tags` (
  `hash_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `admin_only` tinyint(1) NOT NULL,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`hash_id`,`name`,`language_id`),
  KEY `bible_fileset_tags_hash_id_index` (`hash_id`),
  KEY `bible_fileset_tags_iso_index` (`iso`),
  KEY `language_id` (`language_id`),
  CONSTRAINT `FK_bible_filesets_bible_fileset_tags` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_bible_fileset_tags` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`),
  CONSTRAINT `FK_languages_bible_fileset_tags_iso` FOREIGN KEY (`iso`) REFERENCES `languages` (`iso`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_fileset_types`
--

DROP TABLE IF EXISTS `bible_fileset_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_fileset_types` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `set_type_code` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `bible_fileset_types_set_type_code_unique` (`set_type_code`),
  UNIQUE KEY `bible_fileset_types_name_unique` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_filesets`
--

DROP TABLE IF EXISTS `bible_filesets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_filesets` (
  `id` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `asset_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `set_type_code` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `set_size_code` char(9) COLLATE utf8mb4_unicode_ci NOT NULL,
  `hidden` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`hash_id`),
  UNIQUE KEY `unique_prefix_for_s3` (`id`,`asset_id`,`set_type_code`),
  KEY `bible_filesets_bucket_id_foreign` (`asset_id`),
  KEY `bible_filesets_set_type_code_foreign` (`set_type_code`),
  KEY `bible_filesets_set_size_code_foreign` (`set_size_code`),
  KEY `bible_filesets_id_index` (`id`),
  KEY `bible_filesets_hash_id_index` (`hash_id`),
  CONSTRAINT `FK_assets_bible_filesets` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_fileset_sizes_bible_filesets` FOREIGN KEY (`set_size_code`) REFERENCES `bible_fileset_sizes` (`set_size_code`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_fileset_types_bible_filesets` FOREIGN KEY (`set_type_code`) REFERENCES `bible_fileset_types` (`set_type_code`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_links`
--

DROP TABLE IF EXISTS `bible_links`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_links` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `url` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `provider` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `visible` tinyint(1) NOT NULL DEFAULT '1',
  `organization_id` int(10) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `bible_links_bible_id_foreign` (`bible_id`),
  KEY `bible_links_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_bibles_bible_links` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_bible_links` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=52469 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_organizations`
--

DROP TABLE IF EXISTS `bible_organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_organizations` (
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organization_id` int(10) unsigned DEFAULT NULL,
  `relationship_type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `bible_organizations_bible_id_foreign` (`bible_id`),
  KEY `bible_organizations_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_bibles_bible_organizations` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_bible_organizations` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_size_translations`
--

DROP TABLE IF EXISTS `bible_size_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_size_translations` (
  `set_size_code` char(9) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`set_size_code`),
  KEY `bible_size_translations_iso_index` (`iso`),
  CONSTRAINT `FK_bible_fileset_sizes_bible_size_translations` FOREIGN KEY (`set_size_code`) REFERENCES `bible_fileset_sizes` (`set_size_code`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_bible_size_translations` FOREIGN KEY (`iso`) REFERENCES `languages` (`iso`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_translations`
--

DROP TABLE IF EXISTS `bible_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_translations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `vernacular` tinyint(1) NOT NULL DEFAULT '0',
  `vernacular_trade` tinyint(1) NOT NULL DEFAULT '0',
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `background` text COLLATE utf8mb4_unicode_ci,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `bible_translations_language_id_foreign` (`language_id`),
  KEY `bible_translations_bible_id_foreign` (`bible_id`),
  CONSTRAINT `FK_bibles_bible_translations` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_bible_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=8797 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_translator`
--

DROP TABLE IF EXISTS `bible_translator`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_translator` (
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `translator_id` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `bible_translator_translator_id_foreign` (`translator_id`),
  KEY `bible_translator_bible_id_index` (`bible_id`),
  CONSTRAINT `FK_bibles_bible_translator` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_translators_bible_translator` FOREIGN KEY (`translator_id`) REFERENCES `translators` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_verse_concordance`
--

DROP TABLE IF EXISTS `bible_verse_concordance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_verse_concordance` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `bible_verse_id` int(10) unsigned NOT NULL,
  `bible_concordance` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `bible_verse_concordance_bible_verse_id_foreign` (`bible_verse_id`),
  KEY `bible_verse_concordance_bible_concordance_foreign` (`bible_concordance`),
  CONSTRAINT `FK_bible_concordance_bible_verse_concordance` FOREIGN KEY (`bible_concordance`) REFERENCES `bible_concordance` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_bible_verses_bible_verse_concordance` FOREIGN KEY (`bible_verse_id`) REFERENCES `bible_verses` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bible_verses`
--

DROP TABLE IF EXISTS `bible_verses`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bible_verses` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hash_id` char(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chapter` tinyint(3) unsigned NOT NULL,
  `verse_start` tinyint(3) unsigned NOT NULL,
  `verse_end` tinyint(3) unsigned NOT NULL,
  `verse_text` text COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_text_reference` (`hash_id`,`book_id`,`chapter`,`verse_start`),
  KEY `bible_text_book_id_foreign` (`book_id`),
  KEY `bible_text_hash_id_index` (`hash_id`),
  FULLTEXT KEY `verse_text` (`verse_text`),
  CONSTRAINT `bible_text_hash_id_foreign` FOREIGN KEY (`hash_id`) REFERENCES `bible_filesets` (`hash_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=24930887 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bibles`
--

DROP TABLE IF EXISTS `bibles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bibles` (
  `id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `versification` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'protestant',
  `numeral_system_id` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `date` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `scope` char(4) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `script` char(4) COLLATE utf8mb4_unicode_ci NOT NULL,
  `derived` text COLLATE utf8mb4_unicode_ci,
  `copyright` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `reviewed` tinyint(1) DEFAULT '0',
  `notes` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `bibles_id_unique` (`id`),
  KEY `bibles_language_id_foreign` (`language_id`),
  KEY `bibles_numeral_system_id_foreign` (`numeral_system_id`),
  KEY `bibles_script_foreign` (`script`),
  CONSTRAINT `FK_alphabets_bibles` FOREIGN KEY (`script`) REFERENCES `alphabets` (`script`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_bibles` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_numeral_systems_bibles` FOREIGN KEY (`numeral_system_id`) REFERENCES `numeral_systems` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `book_translations`
--

DROP TABLE IF EXISTS `book_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `book_translations` (
  `language_id` int(10) unsigned NOT NULL,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_long` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_short` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_abbreviation` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`language_id`,`book_id`),
  KEY `book_translations_language_id_foreign` (`language_id`),
  KEY `book_translations_book_id_foreign` (`book_id`),
  CONSTRAINT `FK_books_book_translations` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`),
  CONSTRAINT `FK_languages_book_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `books`
--

DROP TABLE IF EXISTS `books`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `books` (
  `id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `id_usfx` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `id_osis` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `book_testament` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `book_group` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chapters` int(10) unsigned DEFAULT NULL,
  `verses` int(10) unsigned DEFAULT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `notes` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `testament_order` tinyint(3) unsigned DEFAULT NULL,
  `protestant_order` tinyint(3) unsigned DEFAULT NULL,
  `luther_order` tinyint(3) unsigned DEFAULT NULL,
  `synodal_order` tinyint(3) unsigned DEFAULT NULL,
  `german_order` tinyint(3) unsigned DEFAULT NULL,
  `kjva_order` tinyint(3) unsigned DEFAULT NULL,
  `vulgate_order` tinyint(3) unsigned DEFAULT NULL,
  `lxx_order` tinyint(3) unsigned DEFAULT NULL,
  `orthodox_order` tinyint(3) unsigned DEFAULT NULL,
  `nrsva_order` tinyint(3) unsigned DEFAULT NULL,
  `catholic_order` tinyint(3) unsigned DEFAULT NULL,
  `finnish_order` tinyint(3) unsigned DEFAULT NULL,
  `messianic_order` tinyint(3) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `commentaries`
--

DROP TABLE IF EXISTS `commentaries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `commentaries` (
  `id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `author` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `features` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publisher` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `commentary_sections`
--

DROP TABLE IF EXISTS `commentary_sections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `commentary_sections` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `commentary_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `content` text COLLATE utf8mb4_unicode_ci,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chapter_start` tinyint(3) unsigned DEFAULT NULL,
  `chapter_end` tinyint(3) unsigned DEFAULT NULL,
  `verse_start` tinyint(3) unsigned DEFAULT NULL,
  `verse_end` tinyint(3) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `FK_commentaries_commentary_sections` (`commentary_id`),
  KEY `FK_books_commentary_sections` (`book_id`),
  CONSTRAINT `FK_books_commentary_sections` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`),
  CONSTRAINT `FK_commentaries_commentary_sections` FOREIGN KEY (`commentary_id`) REFERENCES `commentaries` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=16108 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `commentary_translations`
--

DROP TABLE IF EXISTS `commentary_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `commentary_translations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `commentary_id` varchar(12) COLLATE utf8mb4_unicode_ci NOT NULL,
  `vernacular` tinyint(1) NOT NULL DEFAULT '0',
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `FK_languages_commentary_translations` (`language_id`),
  KEY `FK_commentaries_commentary_translations` (`commentary_id`),
  CONSTRAINT `FK_commentaries_commentary_translations` FOREIGN KEY (`commentary_id`) REFERENCES `commentaries` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_commentary_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `connection_translations`
--

DROP TABLE IF EXISTS `connection_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `connection_translations` (
  `language_id` int(10) unsigned NOT NULL,
  `resource_id` int(10) unsigned NOT NULL,
  `vernacular` tinyint(1) NOT NULL,
  `tag` tinyint(1) NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `connection_translations_language_id_foreign` (`language_id`),
  KEY `connection_translations_resource_id_foreign` (`resource_id`),
  CONSTRAINT `FK_languages_connection_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_resources_connection_translations` FOREIGN KEY (`resource_id`) REFERENCES `resources` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `connections`
--

DROP TABLE IF EXISTS `connections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `connections` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `organization_id` int(10) unsigned NOT NULL,
  `site_url` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `cover_thumbnail` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `connections_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_organizations_connections` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `countries`
--

DROP TABLE IF EXISTS `countries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `countries` (
  `id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `iso_a3` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `fips` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `wfb` tinyint(1) NOT NULL DEFAULT '0',
  `ethnologue` tinyint(1) NOT NULL DEFAULT '0',
  `continent` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `introduction` text COLLATE utf8mb4_unicode_ci,
  `overview` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `countries_iso_a3_unique` (`iso_a3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_communications`
--

DROP TABLE IF EXISTS `country_communications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_communications` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `fixed_phones_total` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `fixed_phones_subs_per_100` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `mobile_phones_total` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `mobile_phones_subs_per_100` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone_system_general_assessment` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone_system_international` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone_system_domestic` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `broadcast_media` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `internet_country_code` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `internet_total_users` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `internet_population_percent` decimal(4,1) unsigned NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_communications_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_communications` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_economy`
--

DROP TABLE IF EXISTS `country_economy`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_economy` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `overview` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_power_parity` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_real_growth` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_per_capita` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_household_consumption` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_consumption` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_investment_in_fixed_capital` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_investment_in_inventories` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_sector_agriculture` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_sector_industry` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gdp_sector_services` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `agriculture_products` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `industries` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `industrial_growth_rate` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force_notes` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force_services` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force_industry` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force_agriculture` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `labor_force_occupation_notes` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `unemployment_rate` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `population_below_poverty` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `household_income_lowest_10` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `household_income_highest_10` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `budget_revenues` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `taxes_revenues` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `budget_net` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `public_debt` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `external_debt` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `fiscal_year` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `inflation_rate` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `central_bank_discount_rate` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `commercial_bank_prime_lending_rate` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `stock_money_narrow` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `stock_money_broad` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `stock_domestic_credit` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `exports_commodities` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `exports_partners` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `imports_commodities` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `imports_partners` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `exchange_rates` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_economy_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_economy` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_energy`
--

DROP TABLE IF EXISTS `country_energy`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_energy` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_production` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_consumption` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_generating_capacity` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_fossil_fuels` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_nuclear` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_hydroelectric` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `electricity_renewable` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `crude_oil_production` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `crude_oil_exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `crude_oil_imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `crude_oil_reserves` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `petrol_production` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `petrol_consumption` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `petrol_exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `petrol_imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `natural_gas_production` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `natural_gas_consumption` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `natural_gas_exports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `natural_gas_imports` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `natural_gas_reserves` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `co2_output` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_energy_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_energy` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_geography`
--

DROP TABLE IF EXISTS `country_geography`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_geography` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `location_description` text COLLATE utf8mb4_unicode_ci,
  `latitude` decimal(10,7) DEFAULT NULL,
  `longitude` decimal(10,7) DEFAULT NULL,
  `mapReferences` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `area_sqkm_total` int(10) unsigned DEFAULT NULL,
  `area_sqkm_land` int(10) unsigned DEFAULT NULL,
  `area_sqkm_water` int(10) unsigned DEFAULT NULL,
  `area_km_coastline` int(10) unsigned DEFAULT NULL,
  `area_note` text COLLATE utf8mb4_unicode_ci,
  `climate` text COLLATE utf8mb4_unicode_ci,
  `terrain` text COLLATE utf8mb4_unicode_ci,
  `hazards` text COLLATE utf8mb4_unicode_ci,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_geography_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_geography` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_government`
--

DROP TABLE IF EXISTS `country_government`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_government` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_etymology` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `conventional_long_form` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `conventional_short_form` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `dependency_status` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `government_type` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `capital` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `capital_coordinates` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `capital_time_zone` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `administrative_divisions` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `administrative_divisions_note` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `independence` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `national_holiday` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `constitution` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legal_system` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `citizenship` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `suffrage` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `executive_chief_of_state` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `executive_head_of_government` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `executive_cabinet` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `executive_elections` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `executive_election_results` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_elections` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_election_results` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_highest_courts` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_judge_selection` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `legislative_subordinate_courts` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `political_parties` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `political_pressure` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `international_organization_participation` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `diplomatic_representation_in_usa` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `diplomatic_representation_from_usa` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `flag_description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `national_symbols` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `national_anthem` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_government_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_government` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_issues`
--

DROP TABLE IF EXISTS `country_issues`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_issues` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `international_disputes` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `illicit_drugs` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `refugees` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_issues_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_issues` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_joshua_project`
--

DROP TABLE IF EXISTS `country_joshua_project`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_joshua_project` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_official_iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_official_name` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `population` bigint(20) unsigned NOT NULL DEFAULT '0',
  `population_unreached` bigint(20) unsigned NOT NULL DEFAULT '0',
  `people_groups` int(10) unsigned NOT NULL DEFAULT '0',
  `people_groups_unreached` int(10) unsigned NOT NULL DEFAULT '0',
  `joshua_project_scale` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `primary_religion` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `percent_christian` double(8,2) DEFAULT NULL,
  `resistant_belt` tinyint(1) NOT NULL DEFAULT '0',
  `percent_literate` double(8,2) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_joshua_project_country_id_foreign` (`country_id`),
  KEY `country_joshua_project_language_official_iso_foreign` (`language_official_iso`),
  CONSTRAINT `FK_countries_country_joshua_project` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_country_joshua_project` FOREIGN KEY (`language_official_iso`) REFERENCES `languages` (`iso`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_language`
--

DROP TABLE IF EXISTS `country_language`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_language` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `population` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `uq_country_language` (`country_id`,`language_id`),
  KEY `country_language_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_countries_country_language` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_country_language` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_maps`
--

DROP TABLE IF EXISTS `country_maps`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_maps` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `thumbnail_url` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `map_url` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_maps_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_maps` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_people`
--

DROP TABLE IF EXISTS `country_people`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_people` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `languages` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `religions` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `population` int(10) unsigned DEFAULT NULL,
  `population_date` int(10) unsigned DEFAULT NULL,
  `nationality_noun` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nationality_adjective` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `age_structure_14` decimal(4,2) unsigned DEFAULT NULL,
  `age_structure_24` decimal(4,2) unsigned DEFAULT NULL,
  `age_structure_54` decimal(4,2) unsigned DEFAULT NULL,
  `age_structure_64` decimal(4,2) unsigned DEFAULT NULL,
  `age_structure_65` decimal(4,2) unsigned DEFAULT NULL,
  `dependency_total` decimal(4,2) unsigned DEFAULT NULL,
  `dependency_youth` decimal(4,2) unsigned DEFAULT NULL,
  `dependency_elder` decimal(4,2) unsigned DEFAULT NULL,
  `dependency_potential` decimal(4,2) unsigned DEFAULT NULL,
  `median_age_total` decimal(3,2) unsigned DEFAULT NULL,
  `median_age_male` decimal(3,2) unsigned DEFAULT NULL,
  `median_age_female` decimal(3,2) unsigned DEFAULT NULL,
  `population_growth_rate_percentage` decimal(3,2) DEFAULT NULL,
  `birth_rate_per_1k` decimal(8,2) unsigned DEFAULT NULL,
  `death_rate_per_1k` decimal(8,2) unsigned DEFAULT NULL,
  `net_migration_per_1k` decimal(6,2) DEFAULT NULL,
  `population_distribution` text COLLATE utf8mb4_unicode_ci,
  `urban_population_percentage` decimal(4,2) unsigned DEFAULT NULL,
  `urbanization_rate` decimal(4,2) unsigned DEFAULT NULL,
  `major_urban_areas_population` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sex_ratio_birth` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_14` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_24` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_54` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_64` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_65` decimal(3,1) unsigned DEFAULT NULL,
  `sex_ratio_total` decimal(3,1) unsigned DEFAULT NULL,
  `mother_age_first_birth` tinyint(3) unsigned DEFAULT NULL,
  `maternal_mortality_rate` decimal(3,1) unsigned DEFAULT NULL,
  `infant_mortality_per_1k_total` decimal(3,2) unsigned DEFAULT NULL,
  `infant_mortality_per_1k_male` decimal(3,2) unsigned DEFAULT NULL,
  `infant_mortality_per_1k_female` decimal(3,2) unsigned DEFAULT NULL,
  `life_expectancy_at_birth_total` decimal(3,1) unsigned DEFAULT NULL,
  `life_expectancy_at_birth_male` decimal(3,1) unsigned DEFAULT NULL,
  `life_expectancy_at_birth_female` decimal(3,1) unsigned DEFAULT NULL,
  `total_fertility_rate` decimal(4,2) DEFAULT NULL,
  `contraceptive_prevalence` decimal(4,2) DEFAULT NULL,
  `health_expenditures` decimal(4,2) DEFAULT NULL,
  `physicians` decimal(4,2) DEFAULT NULL,
  `hospital_beds` decimal(4,2) DEFAULT NULL,
  `drinking_water_source_urban_improved` decimal(5,2) DEFAULT NULL,
  `drinking_water_source_rural_improved` decimal(5,2) DEFAULT NULL,
  `sanitation_facility_access_urban_improved` decimal(5,2) DEFAULT NULL,
  `sanitation_facility_access_rural_improved` decimal(5,2) DEFAULT NULL,
  `hiv_infection_rate` decimal(4,2) DEFAULT NULL,
  `hiv_infected` decimal(4,2) DEFAULT NULL,
  `hiv_deaths` decimal(4,2) DEFAULT NULL,
  `obesity_rate` decimal(4,2) DEFAULT NULL,
  `underweight_children` decimal(4,2) DEFAULT NULL,
  `education_expenditures` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `literacy_definition` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `literacy_total` decimal(5,2) unsigned DEFAULT NULL,
  `literacy_male` decimal(5,2) unsigned DEFAULT NULL,
  `literacy_female` decimal(5,2) unsigned DEFAULT NULL,
  `school_years_total` tinyint(3) unsigned DEFAULT NULL,
  `school_years_male` tinyint(3) unsigned DEFAULT NULL,
  `school_years_female` tinyint(3) unsigned DEFAULT NULL,
  `child_labor` int(10) unsigned DEFAULT NULL,
  `child_labor_percentage` decimal(4,2) unsigned DEFAULT NULL,
  `unemployment_youth_total` decimal(4,2) unsigned DEFAULT NULL,
  `unemployment_youth_male` decimal(4,2) unsigned DEFAULT NULL,
  `unemployment_youth_female` decimal(4,2) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`country_id`),
  KEY `country_people_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_people` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_people_ethnicities`
--

DROP TABLE IF EXISTS `country_people_ethnicities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_people_ethnicities` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `population_percentage` decimal(5,2) unsigned NOT NULL,
  `date` tinyint(3) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_people_ethnicities_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_people_ethnicities` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_regions`
--

DROP TABLE IF EXISTS `country_regions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_regions` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_regions_country_id_foreign` (`country_id`),
  KEY `country_regions_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_countries_country_regions` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_country_regions` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_religions`
--

DROP TABLE IF EXISTS `country_religions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_religions` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `population_percentage` decimal(5,2) unsigned DEFAULT NULL,
  `date` tinyint(3) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_religions_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_religions` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_translations`
--

DROP TABLE IF EXISTS `country_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_translations` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_translations_country_id_foreign` (`country_id`),
  KEY `country_translations_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_countries_country_translations` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_country_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `country_transportation`
--

DROP TABLE IF EXISTS `country_transportation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country_transportation` (
  `country_id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `air_carriers` int(10) unsigned DEFAULT NULL,
  `aircraft` int(10) unsigned DEFAULT NULL,
  `aircraft_passengers` int(10) unsigned DEFAULT NULL,
  `aircraft_freight` int(10) unsigned DEFAULT NULL,
  `aircraft_code_prefix` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `airports` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `airports_paved` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `airports_info_date` tinyint(3) unsigned DEFAULT NULL,
  `major_seaports` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `oil_terminals` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cruise_ports` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `country_transportation_country_id_foreign` (`country_id`),
  CONSTRAINT `FK_countries_country_transportation` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_bibleInfo`
--

DROP TABLE IF EXISTS `language_bibleInfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_bibleInfo` (
  `language_id` int(10) unsigned NOT NULL,
  `bible_status` tinyint(4) DEFAULT NULL,
  `bible_translation_need` tinyint(1) DEFAULT NULL,
  `bible_year` int(11) DEFAULT NULL,
  `bible_year_newTestament` int(11) DEFAULT NULL,
  `bible_year_portions` int(11) DEFAULT NULL,
  `bible_sample_text` text COLLATE utf8mb4_unicode_ci,
  `bible_sample_img` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `language_bibleinfo_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_languages_language_bibleInfo` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_classifications`
--

DROP TABLE IF EXISTS `language_classifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_classifications` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `classification_id` char(8) COLLATE utf8mb4_unicode_ci NOT NULL,
  `order` tinyint(3) unsigned NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `language_classifications_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_languages_language_classifications` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=45209 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_codes`
--

DROP TABLE IF EXISTS `language_codes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_codes` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `source` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `code` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `language_codes_language_id_foreign` (`language_id`),
  CONSTRAINT `FK_languages_language_codes` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1752 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_dialects`
--

DROP TABLE IF EXISTS `language_dialects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_dialects` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `dialect_id` char(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `language_dialects_language_id_foreign` (`language_id`),
  KEY `language_dialects_dialect_id_index` (`dialect_id`),
  CONSTRAINT `FK_languages_language_dialects` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=445 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_status`
--

DROP TABLE IF EXISTS `language_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_status` (
  `id` char(2) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `language_translations`
--

DROP TABLE IF EXISTS `language_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `language_translations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_source_id` int(10) unsigned NOT NULL,
  `language_translation_id` int(10) unsigned NOT NULL,
  `name` varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `priority` tinyint(4) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_language_translations` (`language_source_id`,`language_translation_id`,`name`),
  KEY `language_translations_language_source_id_foreign` (`language_source_id`),
  KEY `language_translations_language_translation_id_foreign` (`language_translation_id`),
  CONSTRAINT `FK_languages_language_translations_language_source_id` FOREIGN KEY (`language_source_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_language_translations_language_tranlation_id` FOREIGN KEY (`language_translation_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=85109 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `languages`
--

DROP TABLE IF EXISTS `languages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `languages` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `glotto_id` char(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `iso2B` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso2T` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso1` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `maps` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `development` text COLLATE utf8mb4_unicode_ci,
  `use` text COLLATE utf8mb4_unicode_ci,
  `location` text COLLATE utf8mb4_unicode_ci,
  `area` text COLLATE utf8mb4_unicode_ci,
  `population` int(11) DEFAULT NULL,
  `population_notes` text COLLATE utf8mb4_unicode_ci,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `typology` text COLLATE utf8mb4_unicode_ci,
  `writing` text COLLATE utf8mb4_unicode_ci,
  `latitude` double(11,7) DEFAULT NULL,
  `longitude` double(11,7) DEFAULT NULL,
  `status_id` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country_id` char(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `languages_glotto_id_unique` (`glotto_id`),
  UNIQUE KEY `languages_iso2b_unique` (`iso2B`),
  UNIQUE KEY `languages_iso2t_unique` (`iso2T`),
  UNIQUE KEY `languages_iso1_unique` (`iso1`),
  KEY `languages_iso_index` (`iso`),
  KEY `language_status_foreign_key` (`status_id`),
  KEY `country_id_foreign_key` (`country_id`),
  CONSTRAINT `FK_countries_languages` FOREIGN KEY (`country_id`) REFERENCES `countries` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `FK_language_status_languages` FOREIGN KEY (`status_id`) REFERENCES `language_status` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=8180 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `numeral_system_glyphs`
--

DROP TABLE IF EXISTS `numeral_system_glyphs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `numeral_system_glyphs` (
  `numeral_system_id` char(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `value` tinyint(3) unsigned NOT NULL,
  `glyph` varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL,
  `numeral_written` varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uq_numeral_system_glyph` (`numeral_system_id`,`value`,`glyph`),
  KEY `numeral_system_glyphs_numeral_system_id_index` (`numeral_system_id`),
  CONSTRAINT `FK_numeral_systems_numeral_system_glyphs` FOREIGN KEY (`numeral_system_id`) REFERENCES `numeral_systems` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `numeral_systems`
--

DROP TABLE IF EXISTS `numeral_systems`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `numeral_systems` (
  `id` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organization_logos`
--

DROP TABLE IF EXISTS `organization_logos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organization_logos` (
  `organization_id` int(10) unsigned NOT NULL,
  `language_id` int(10) unsigned NOT NULL,
  `language_iso` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `icon` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`organization_id`,`language_id`,`icon`),
  KEY `organization_logos_organization_id_foreign` (`organization_id`),
  KEY `organization_logos_language_id_foreign` (`language_id`),
  KEY `organization_logos_language_iso_foreign` (`language_iso`),
  CONSTRAINT `FK_languages_organization_logos_language_id` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_organization_logos_language_iso` FOREIGN KEY (`language_iso`) REFERENCES `languages` (`iso`),
  CONSTRAINT `FK_organizations_organization_logos` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organization_relationships`
--

DROP TABLE IF EXISTS `organization_relationships`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organization_relationships` (
  `organization_parent_id` int(10) unsigned NOT NULL,
  `organization_child_id` int(10) unsigned NOT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`organization_parent_id`,`organization_child_id`),
  KEY `organization_relationships_organization_parent_id_foreign` (`organization_parent_id`),
  KEY `organization_relationships_organization_child_id_foreign` (`organization_child_id`),
  CONSTRAINT `FK_organizations_organization_relationships_organization_child` FOREIGN KEY (`organization_child_id`) REFERENCES `organizations` (`id`),
  CONSTRAINT `FK_organizations_organization_relationships_organization_parent` FOREIGN KEY (`organization_parent_id`) REFERENCES `organizations` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organization_translations`
--

DROP TABLE IF EXISTS `organization_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organization_translations` (
  `language_id` int(10) unsigned NOT NULL,
  `organization_id` int(10) unsigned NOT NULL,
  `vernacular` tinyint(1) NOT NULL DEFAULT '0',
  `alt` tinyint(1) NOT NULL DEFAULT '0',
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `description_short` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`language_id`,`organization_id`,`name`),
  KEY `organization_translations_language_id_foreign` (`language_id`),
  KEY `organization_translations_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_languages_organization_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_organization_translations` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organizations`
--

DROP TABLE IF EXISTS `organizations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `organizations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `slug` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `abbreviation` char(6) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `notes` text COLLATE utf8mb4_unicode_ci,
  `primaryColor` varchar(7) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `secondaryColor` varchar(7) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `inactive` tinyint(1) DEFAULT '0',
  `url_facebook` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url_website` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url_donate` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url_twitter` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address2` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `zip` int(10) unsigned DEFAULT NULL,
  `phone` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email_director` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` double(11,7) DEFAULT NULL,
  `longitude` double(11,7) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `organizations_slug_unique` (`slug`),
  UNIQUE KEY `organizations_abbreviation_unique` (`abbreviation`),
  KEY `organizations_country_foreign` (`country`),
  CONSTRAINT `FK_countries_organizations` FOREIGN KEY (`country`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1154 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `resource_connections`
--

DROP TABLE IF EXISTS `resource_connections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `resource_connections` (
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `resource_links`
--

DROP TABLE IF EXISTS `resource_links`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `resource_links` (
  `resource_id` int(10) unsigned NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `size` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `url` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `resource_links_resource_id_foreign` (`resource_id`),
  CONSTRAINT `FK_resources_resource_links` FOREIGN KEY (`resource_id`) REFERENCES `resources` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `resource_translations`
--

DROP TABLE IF EXISTS `resource_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `resource_translations` (
  `language_id` int(10) unsigned NOT NULL,
  `resource_id` int(10) unsigned NOT NULL,
  `vernacular` tinyint(1) NOT NULL,
  `tag` tinyint(1) NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`language_id`,`resource_id`),
  KEY `resource_translations_language_id_foreign` (`language_id`),
  KEY `resource_translations_resource_id_foreign` (`resource_id`),
  CONSTRAINT `FK_languages_resource_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_resources_resource_translations` FOREIGN KEY (`resource_id`) REFERENCES `resources` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `resources`
--

DROP TABLE IF EXISTS `resources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `resources` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned NOT NULL,
  `slug` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso` char(3) COLLATE utf8mb4_unicode_ci NOT NULL,
  `organization_id` int(10) unsigned NOT NULL,
  `source_id` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cover` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cover_thumbnail` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `resources_language_id_foreign` (`language_id`),
  KEY `resources_organization_id_foreign` (`organization_id`),
  KEY `resources_iso_index` (`iso`),
  CONSTRAINT `FK_languages_resources_iso` FOREIGN KEY (`iso`) REFERENCES `languages` (`iso`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_resources_language_id` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_resources` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=149096 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `translator_relations`
--

DROP TABLE IF EXISTS `translator_relations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `translator_relations` (
  `translator_id` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `translator_relation_id` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `organization_id` int(10) unsigned DEFAULT NULL,
  `type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `notes` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `translator_relations_translator_id_foreign` (`translator_id`),
  KEY `translator_relations_translator_relation_id_foreign` (`translator_relation_id`),
  KEY `translator_relations_organization_id_foreign` (`organization_id`),
  CONSTRAINT `FK_organizations_translator_relations` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`),
  CONSTRAINT `FK_translaors_translator_relations_translator_relation_id` FOREIGN KEY (`translator_relation_id`) REFERENCES `translators` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_translators_translator_relations` FOREIGN KEY (`translator_id`) REFERENCES `translators` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `translators`
--

DROP TABLE IF EXISTS `translators`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `translators` (
  `id` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `born` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `died` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `video_sources`
--

DROP TABLE IF EXISTS `video_sources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `video_sources` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `video_id` int(10) unsigned DEFAULT NULL,
  `url` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `encoding` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `resolution` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `size` int(11) NOT NULL,
  `url_type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `video_sources_video_id_foreign` (`video_id`),
  CONSTRAINT `FK_videos_video_sources` FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `video_tags`
--

DROP TABLE IF EXISTS `video_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `video_tags` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `video_id` int(10) unsigned DEFAULT NULL,
  `category` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `tag_type` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `tag` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `language_id` int(10) unsigned DEFAULT NULL,
  `organization_id` int(10) unsigned DEFAULT NULL,
  `book_id` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chapter_start` int(10) unsigned DEFAULT NULL,
  `chapter_end` int(10) unsigned DEFAULT NULL,
  `verse_start` int(10) unsigned DEFAULT NULL,
  `verse_end` int(10) unsigned DEFAULT NULL,
  `time_begin` double(8,2) unsigned DEFAULT NULL,
  `time_end` double(8,2) unsigned DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `video_tags_video_id_foreign` (`video_id`),
  KEY `video_tags_language_id_foreign` (`language_id`),
  KEY `video_tags_organization_id_foreign` (`organization_id`),
  KEY `video_tags_book_id_foreign` (`book_id`),
  CONSTRAINT `FK_books_video_tags` FOREIGN KEY (`book_id`) REFERENCES `books` (`id`),
  CONSTRAINT `FK_languages_video_tags` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_organizations_video_tags` FOREIGN KEY (`organization_id`) REFERENCES `organizations` (`id`),
  CONSTRAINT `FK_videos_video_tags` FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `video_translations`
--

DROP TABLE IF EXISTS `video_translations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `video_translations` (
  `language_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `video_id` int(10) unsigned NOT NULL,
  `title` varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`language_id`),
  KEY `video_translations_video_id_foreign` (`video_id`),
  CONSTRAINT `FK_languages_video_translations` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_videos_video_translations` FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `videos`
--

DROP TABLE IF EXISTS `videos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `videos` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `language_id` int(10) unsigned DEFAULT NULL,
  `bible_id` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `series` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `episode` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `section` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `picture` varchar(191) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `duration` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `videos_language_id_foreign` (`language_id`),
  KEY `videos_bible_id_foreign` (`bible_id`),
  CONSTRAINT `FK_bibles_videos` FOREIGN KEY (`bible_id`) REFERENCES `bibles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `FK_languages_videos` FOREIGN KEY (`language_id`) REFERENCES `languages` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-07-26 19:17:00
