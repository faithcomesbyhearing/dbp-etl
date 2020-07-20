CREATE TEMPORARY TABLE `organizations_temp` (
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
  -- `lpts_licensor` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `organizations_slug_unique_temp` (`slug`),
  UNIQUE KEY `organizations_abbreviation_unique_temp` (`abbreviation`),
  KEY `organizations_country_foreign_temp` (`country`)
  -- CONSTRAINT `FK_countries_organizations_temp` FOREIGN KEY (`country`) REFERENCES `countries` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1168 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci



LOAD DATA INFILE '/tmp/query_result.csv' 
INTO TABLE organizations_temp
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SET FOREIGN_KEY_CHECKS=0;


replace into organizations
select *, NULL from organizations_temp;

SET FOREIGN_KEY_CHECKS=1;

