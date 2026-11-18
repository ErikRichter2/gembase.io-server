
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Dumping database structure for gb_app
CREATE DATABASE IF NOT EXISTS `gb_app` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_app`;

-- Dumping structure for table gb_app.config
CREATE TABLE IF NOT EXISTS `config` (
  `gr` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `value` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`gr`(10),`id`(10)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.customers
CREATE TABLE IF NOT EXISTS `customers` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `data` json NOT NULL,
  `deleted` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=320 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_allowed_tags_per_locked_module
CREATE TABLE IF NOT EXISTS `def_allowed_tags_per_locked_module` (
  `module_id` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `is_set` tinyint NOT NULL DEFAULT '0',
  `is_changeable` tinyint NOT NULL DEFAULT '0',
  `is_loved` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`module_id`,`tag_id_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_competitor_tags_parameters
CREATE TABLE IF NOT EXISTS `def_competitor_tags_parameters` (
  `id` int NOT NULL,
  `name` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_countries
CREATE TABLE IF NOT EXISTS `def_countries` (
  `country_id` int NOT NULL,
  `survey_cpi` decimal(8,2) NOT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `code_2` char(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `code_3` char(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `vat_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `vat_abbreviation` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `vat_characters_min` int unsigned NOT NULL DEFAULT '0',
  `vat_characters_max` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`country_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_cron
CREATE TABLE IF NOT EXISTS `def_cron` (
  `id` int NOT NULL AUTO_INCREMENT,
  `cron_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `priority` int NOT NULL DEFAULT '0',
  `last_run` timestamp NULL DEFAULT NULL,
  `state` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `log` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_email_templates
CREATE TABLE IF NOT EXISTS `def_email_templates` (
  `id` int NOT NULL AUTO_INCREMENT,
  `template_def` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `subject` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `title` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `body` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `footer` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `def_email_teplates_pk` (`template_def`(50)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_prime_numbers
CREATE TABLE IF NOT EXISTS `def_prime_numbers` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_server_services
CREATE TABLE IF NOT EXISTS `def_server_services` (
  `id` int NOT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `command` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_age_groups
CREATE TABLE IF NOT EXISTS `def_sheet_platform_age_groups` (
  `id` int NOT NULL AUTO_INCREMENT,
  `age_from` int NOT NULL,
  `age_to` int NOT NULL,
  `group_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=313 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_modules_pricing
CREATE TABLE IF NOT EXISTS `def_sheet_platform_modules_pricing` (
  `id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `price` decimal(8,2) NOT NULL,
  PRIMARY KEY (`id`(20)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_product
CREATE TABLE IF NOT EXISTS `def_sheet_platform_product` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tag_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `tag_id_int` int NOT NULL DEFAULT '0',
  `category` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `subcategory` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `node` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `weight` int NOT NULL DEFAULT '0',
  `is_prompt` int NOT NULL DEFAULT '0',
  `is_survey` tinyint unsigned NOT NULL DEFAULT '0',
  `competitors_pool_w` int NOT NULL DEFAULT '0',
  `threatscore_similarity_w` int NOT NULL DEFAULT '0',
  `adj` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `subcategory_int` int NOT NULL DEFAULT '0',
  `description` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `parent_genre_for_core_tag_id_int` int unsigned NOT NULL DEFAULT '0',
  `unlocked` tinyint NOT NULL DEFAULT '0',
  `hidden` int NOT NULL DEFAULT '0',
  `subgenre` int NOT NULL DEFAULT '0',
  `loved_ratio_ext` decimal(8,4) NOT NULL DEFAULT '0.0000',
  `not_rejected` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=20591 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_spending_groups
CREATE TABLE IF NOT EXISTS `def_sheet_platform_spending_groups` (
  `id` int unsigned NOT NULL,
  `group_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `spending_from` int NOT NULL,
  `spending_to` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_threats_params
CREATE TABLE IF NOT EXISTS `def_sheet_platform_threats_params` (
  `id` int NOT NULL AUTO_INCREMENT,
  `param` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `v1` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `v2` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `v3` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `color` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1450 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_threats_similarity
CREATE TABLE IF NOT EXISTS `def_sheet_platform_threats_similarity` (
  `id` int NOT NULL AUTO_INCREMENT,
  `subcategory` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `subcategory_int` int NOT NULL DEFAULT '0',
  `subcategory_weight` int NOT NULL,
  `similar_count` int NOT NULL,
  `weight` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=56 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_values_constants
CREATE TABLE IF NOT EXISTS `def_sheet_platform_values_constants` (
  `id` int NOT NULL AUTO_INCREMENT,
  `param` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `v1` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=206 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_platform_values_install_tiers
CREATE TABLE IF NOT EXISTS `def_sheet_platform_values_install_tiers` (
  `tier` int NOT NULL,
  `store_id` int NOT NULL,
  `value_from` bigint NOT NULL,
  `value_to` bigint NOT NULL,
  PRIMARY KEY (`tier`,`store_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_sheet_subcategory_goals
CREATE TABLE IF NOT EXISTS `def_sheet_subcategory_goals` (
  `subcategory_int` int NOT NULL,
  `goal_value` decimal(5,2) NOT NULL DEFAULT '0.00',
  PRIMARY KEY (`subcategory_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_store_apps_categories
CREATE TABLE IF NOT EXISTS `def_store_apps_categories` (
  `id` int NOT NULL,
  `value` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `store_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_studies_dcm_concepts
CREATE TABLE IF NOT EXISTS `def_studies_dcm_concepts` (
  `headers` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `features` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_studies_traits
CREATE TABLE IF NOT EXISTS `def_studies_traits` (
  `id` int unsigned NOT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `study_default` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for view gb_app.def_tags_behaviors_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `def_tags_behaviors_v` (
	`tag_id_int` INT NOT NULL
);

-- Dumping structure for view gb_app.def_tags_genres_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `def_tags_genres_v` (
	`tag_id_int` INT NOT NULL
);

-- Dumping structure for view gb_app.def_tags_needs_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `def_tags_needs_v` (
	`tag_id_int` INT NOT NULL
);

-- Dumping structure for view gb_app.def_tags_topics_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `def_tags_topics_v` (
	`tag_id_int` INT NOT NULL
);

-- Dumping structure for table gb_app.def_tools
CREATE TABLE IF NOT EXISTS `def_tools` (
  `id` int NOT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_user_position
CREATE TABLE IF NOT EXISTS `def_user_position` (
  `id` int NOT NULL AUTO_INCREMENT,
  `position` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `position_area` int NOT NULL,
  `position_role` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_user_position_area
CREATE TABLE IF NOT EXISTS `def_user_position_area` (
  `id` int NOT NULL,
  `value` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_user_position_role
CREATE TABLE IF NOT EXISTS `def_user_position_role` (
  `id` int NOT NULL,
  `value` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.def_user_roles
CREATE TABLE IF NOT EXISTS `def_user_roles` (
  `id` int NOT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.dms
CREATE TABLE IF NOT EXISTS `dms` (
  `id` int NOT NULL AUTO_INCREMENT,
  `location` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `file_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `file_type` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `file_desc` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `user_id` int NOT NULL DEFAULT '0',
  `mime` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `folder` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `version` int NOT NULL DEFAULT '1',
  `is_binary` int NOT NULL DEFAULT '0',
  `mime_subtype` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `app_dms_guid__index` (`guid`(10)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for function gb_app.get_id_from_sequence
DELIMITER //
CREATE FUNCTION `get_id_from_sequence`() RETURNS int
    DETERMINISTIC
BEGIN
    DECLARE res INT;
    SET res = 0;
    UPDATE gb_app.id_sequencer SET id=LAST_INSERT_ID(id+1);
    SELECT LAST_INSERT_ID() INTO res;
    RETURN res;
END//
DELIMITER ;

-- Dumping structure for table gb_app.id_sequencer
CREATE TABLE IF NOT EXISTS `id_sequencer` (
  `id` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_app_id
CREATE TABLE IF NOT EXISTS `map_app_id` (
  `id` int NOT NULL,
  `app_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `map_app_id_app_id_index` (`app_id`(10)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_app_id_to_store_id
CREATE TABLE IF NOT EXISTS `map_app_id_to_store_id` (
  `app_id_int` mediumint NOT NULL,
  `store` tinyint unsigned NOT NULL,
  `app_id_in_store` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`app_id_int`,`store`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_country_id
CREATE TABLE IF NOT EXISTS `map_country_id` (
  `id` int NOT NULL AUTO_INCREMENT,
  `country` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `map_country_id_country_uindex` (`country`(10)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=253 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_dev_id
CREATE TABLE IF NOT EXISTS `map_dev_id` (
  `id` int NOT NULL,
  `dev_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `map_dev_id_dev_id_index` (`dev_id`(10)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_studies_traits
CREATE TABLE IF NOT EXISTS `map_studies_traits` (
  `id` int NOT NULL AUTO_INCREMENT,
  `trait_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `map_studies_traits_trait_id_index` (`trait_id`(10)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_tag_id
CREATE TABLE IF NOT EXISTS `map_tag_id` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tag_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=532 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_tag_subcategory
CREATE TABLE IF NOT EXISTS `map_tag_subcategory` (
  `id` int NOT NULL AUTO_INCREMENT,
  `subcategory` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `client_name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=65 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.map_user_email
CREATE TABLE IF NOT EXISTS `map_user_email` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `map_user_email_email_uindex` (`email`(50)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1179 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for function gb_app.now
DELIMITER //
CREATE FUNCTION `now`() RETURNS date
    DETERMINISTIC
BEGIN
	RETURN '2024-12-18';
END//
DELIMITER ;

-- Dumping structure for table gb_app.organization
CREATE TABLE IF NOT EXISTS `organization` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dev_id_int` int NOT NULL DEFAULT '0',
  `prime_number` int NOT NULL,
  `credits` decimal(10,4) NOT NULL DEFAULT '0.0000',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.organization_domains
CREATE TABLE IF NOT EXISTS `organization_domains` (
  `id` int NOT NULL AUTO_INCREMENT,
  `organization_id` int NOT NULL,
  `domain` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `organization_domains_domain_uindex` (`domain`(50)) USING BTREE,
  KEY `organization_domains_organization_id_index` (`organization_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.organization_requests
CREATE TABLE IF NOT EXISTS `organization_requests` (
  `request_id` int NOT NULL AUTO_INCREMENT,
  `organization_id` int NOT NULL,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `request_guid` char(37) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `created_t` timestamp NOT NULL DEFAULT (now()),
  `sent_request_t` timestamp NULL DEFAULT NULL,
  `from_user_id` int NOT NULL,
  `role` enum('member','admin') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'member',
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `position_role` int NOT NULL DEFAULT '0',
  `position_area` int NOT NULL DEFAULT '0',
  `temp_user` int NOT NULL DEFAULT '0',
  `locked` int NOT NULL DEFAULT '0',
  `confirmed` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`request_id`) USING BTREE,
  UNIQUE KEY `organization_requests_organization_id_email_uindex` (`organization_id`,`email`(50)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.organization_requests_modules
CREATE TABLE IF NOT EXISTS `organization_requests_modules` (
  `request_id` int NOT NULL,
  `billing_id` int NOT NULL,
  `module_id` int NOT NULL,
  PRIMARY KEY (`request_id`,`billing_id`,`module_id`) USING BTREE,
  UNIQUE KEY `organization_requests_modules_request_id_module_id_uindex` (`request_id`,`module_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.organization_users
CREATE TABLE IF NOT EXISTS `organization_users` (
  `organization_id` int NOT NULL,
  `user_id` int NOT NULL,
  `role` enum('admin','member') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'member',
  `active` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`user_id`) USING BTREE,
  KEY `organization_users_organization_id_index` (`organization_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.organization_users_modules
CREATE TABLE IF NOT EXISTS `organization_users_modules` (
  `organization_id` int NOT NULL,
  `user_id` int NOT NULL,
  `billing_id` int NOT NULL,
  `module_id` int NOT NULL,
  PRIMARY KEY (`organization_id`,`user_id`,`billing_id`,`module_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.portal
CREATE TABLE IF NOT EXISTS `portal` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `data` json DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.registration_whitelist_pending
CREATE TABLE IF NOT EXISTS `registration_whitelist_pending` (
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  `ip_address` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `request_guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `confirmed` int NOT NULL DEFAULT '0',
  `responded_t` timestamp NULL DEFAULT NULL,
  `blocked` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`email`(50)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.surveys
CREATE TABLE IF NOT EXISTS `surveys` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dms_id` int NOT NULL DEFAULT '0',
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `is_active` int NOT NULL DEFAULT '0',
  `survey_data` json DEFAULT NULL,
  `survey_config` json DEFAULT NULL,
  `guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `survey_results` json DEFAULT NULL,
  `state` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `is_panel` int NOT NULL DEFAULT '1',
  `is_dcm` int NOT NULL DEFAULT '1',
  `config_dms_id` int NOT NULL DEFAULT '0',
  `langs_dms_id` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users
CREATE TABLE IF NOT EXISTS `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `credits` decimal(10,4) NOT NULL DEFAULT '0.0000',
  `concepts_counter` int NOT NULL DEFAULT '0',
  `prime_number` int unsigned NOT NULL DEFAULT '0',
  `position_area` int NOT NULL DEFAULT '0',
  `position_role` int NOT NULL DEFAULT '0',
  `tos_agree_t` timestamp NULL DEFAULT NULL,
  `temp` int NOT NULL DEFAULT '0',
  `fake_login` int NOT NULL DEFAULT '0',
  `t` timestamp NOT NULL DEFAULT (now()),
  `added_to_my_apps` int NOT NULL DEFAULT '0',
  `tutorial_finished` int NOT NULL DEFAULT '0',
  `free_trial_end_t` timestamp NULL DEFAULT NULL,
  `removed_initial_app` int NOT NULL DEFAULT '0',
  `session_t` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=49 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_apps
CREATE TABLE IF NOT EXISTS `users_apps` (
  `user_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `unlocked_in_demo` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`user_id`,`app_id_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_devs
CREATE TABLE IF NOT EXISTS `users_devs` (
  `user_id` int NOT NULL,
  `dev_id_int` int NOT NULL,
  PRIMARY KEY (`user_id`,`dev_id_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_email_draft
CREATE TABLE IF NOT EXISTS `users_email_draft` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email_id` int NOT NULL,
  `subject` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `template_def` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `template_parameters` json DEFAULT NULL,
  `content_parameters` json DEFAULT NULL,
  `draft_parameters` json DEFAULT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  `from_address` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `users_email_draft_email_id_index` (`email_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=114 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_email_subscription
CREATE TABLE IF NOT EXISTS `users_email_subscription` (
  `email_id` int NOT NULL,
  `request_guid` char(37) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `subscribed` tinyint NOT NULL DEFAULT '0',
  `t` timestamp NOT NULL DEFAULT (now()),
  PRIMARY KEY (`email_id`) USING BTREE,
  UNIQUE KEY `users_email_subscription_request_guid_uindex` (`request_guid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_permissions
CREATE TABLE IF NOT EXISTS `users_permissions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `permission_id` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_pricing
CREATE TABLE IF NOT EXISTS `users_pricing` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `operation_type` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `credits` decimal(10,4) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_registration_requests
CREATE TABLE IF NOT EXISTS `users_registration_requests` (
  `guid` char(37) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `created_t` timestamp NOT NULL DEFAULT (now()),
  `sent_request_t` timestamp NULL DEFAULT NULL,
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `dev_id_int` int NOT NULL DEFAULT '0',
  `position_role` int NOT NULL DEFAULT '0',
  `position_area` int NOT NULL DEFAULT '0',
  `temp_user` int NOT NULL DEFAULT '0',
  `locked` int NOT NULL DEFAULT '0',
  `credits` int NOT NULL DEFAULT '0',
  `organization_domain` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `organization_role` enum('admin','member') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `free_trial_end_t` timestamp NULL DEFAULT NULL,
  `responded_t` timestamp NULL DEFAULT NULL,
  `blocked` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`guid`) USING BTREE,
  UNIQUE KEY `users_registration_requests_email_uindex` (`email`(50)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_studies
CREATE TABLE IF NOT EXISTS `users_studies` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  `name` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `audiences` json DEFAULT NULL,
  `dcm_concepts` json DEFAULT NULL,
  `state` enum('edit','preparing','working','paused','done','error') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `progress_perc` int NOT NULL DEFAULT '0',
  `internal_respondents` int NOT NULL DEFAULT '0',
  `global_study` tinyint(1) NOT NULL DEFAULT '0',
  `default_study` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `users_studies_user_id_index` (`user_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=73 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_studies_respondents_guids
CREATE TABLE IF NOT EXISTS `users_studies_respondents_guids` (
  `respondent_guid` char(37) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `study_id` int NOT NULL,
  `audience_guid` char(37) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`respondent_guid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_tags_override_requests
CREATE TABLE IF NOT EXISTS `users_tags_override_requests` (
  `request_id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  `app_id_int` int NOT NULL,
  `state` enum('pending','accepted','rejected') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `rejected_reason` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`request_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_tags_override_requests_tags
CREATE TABLE IF NOT EXISTS `users_tags_override_requests_tags` (
  `request_id` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `tags_before` int NOT NULL DEFAULT '0',
  `tag_rank` int NOT NULL,
  PRIMARY KEY (`request_id`,`tags_before`,`tag_id_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_tutorial
CREATE TABLE IF NOT EXISTS `users_tutorial` (
  `user_id` int NOT NULL,
  `tutorial` json DEFAULT NULL,
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_app.users_whitelist
CREATE TABLE IF NOT EXISTS `users_whitelist` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `data` json DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `def_tags_behaviors_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `def_tags_behaviors_v` AS select `p`.`tag_id_int` AS `tag_id_int` from (`def_sheet_platform_product` `p` join `map_tag_subcategory` `m`) where ((`p`.`subcategory_int` = `m`.`id`) and (`m`.`subcategory` = 'Behaviors'))
;

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `def_tags_genres_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `def_tags_genres_v` AS select `p`.`tag_id_int` AS `tag_id_int` from (`def_sheet_platform_product` `p` join `map_tag_subcategory` `m`) where ((`p`.`subcategory_int` = `m`.`id`) and (`m`.`subcategory` = 'Genre'))
;

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `def_tags_needs_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `def_tags_needs_v` AS select `p`.`tag_id_int` AS `tag_id_int` from (`def_sheet_platform_product` `p` join `map_tag_subcategory` `m`) where ((`p`.`subcategory_int` = `m`.`id`) and (`m`.`subcategory` = 'Needs'))
;

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `def_tags_topics_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `def_tags_topics_v` AS select `p`.`tag_id_int` AS `tag_id_int` from (`def_sheet_platform_product` `p` join `map_tag_subcategory` `m`) where ((`p`.`subcategory_int` = `m`.`id`) and (`m`.`subcategory` = 'Topics'))
;


-- Dumping database structure for gb_app_temp_data
CREATE DATABASE IF NOT EXISTS `gb_app_temp_data` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_app_temp_data`;

-- Dumping structure for table gb_app_temp_data.erik_monetization_fix
CREATE TABLE IF NOT EXISTS `erik_monetization_fix` (
  `app_id_int` int NOT NULL,
  `state` int NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.erik_store_tags_fix
CREATE TABLE IF NOT EXISTS `erik_store_tags_fix` (
  `app_id_int` int NOT NULL,
  `state` int NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.erik_store_tags_fix_07_09_24
CREATE TABLE IF NOT EXISTS `erik_store_tags_fix_07_09_24` (
  `app_id_int` int NOT NULL,
  `prompt_row_id` int NOT NULL DEFAULT '0',
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL DEFAULT '0',
  `added_from_store` tinyint NOT NULL DEFAULT '0',
  `removed_from_store` tinyint NOT NULL DEFAULT '0',
  `is_tag_rank_override` tinyint NOT NULL DEFAULT '0',
  `tag_rank_override` smallint NOT NULL DEFAULT '0',
  `overriden_by_user` int NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.portal_scrap_dev_id_queue
CREATE TABLE IF NOT EXISTS `portal_scrap_dev_id_queue` (
  `user_id` int NOT NULL,
  `dev_id_int` int NOT NULL,
  `dev_name` tinytext,
  `store` int NOT NULL,
  `dev_id` tinytext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.portal_temp_concepts
CREATE TABLE IF NOT EXISTS `portal_temp_concepts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=66 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.server_services
CREATE TABLE IF NOT EXISTS `server_services` (
  `id` int NOT NULL AUTO_INCREMENT,
  `pid` int NOT NULL,
  `guid` tinytext NOT NULL,
  `def_id` int NOT NULL,
  `script` tinytext,
  `user_id` int NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `status` tinytext,
  `heartbeat` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `payload` tinytext,
  `error` int NOT NULL DEFAULT '0',
  `error_message` tinytext,
  `finished` int NOT NULL DEFAULT '0',
  `log` mediumtext,
  `force_stop` int NOT NULL DEFAULT '0',
  `input_data` tinytext,
  `heartbeat_child` timestamp NULL DEFAULT NULL,
  `service_data` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13371 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.server_services_conn
CREATE TABLE IF NOT EXISTS `server_services_conn` (
  `pid` int NOT NULL,
  `conn_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=MEMORY DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.service_survey_dcm_larch_model
CREATE TABLE IF NOT EXISTS `service_survey_dcm_larch_model` (
  `id` int NOT NULL AUTO_INCREMENT,
  `guid` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `state` int NOT NULL DEFAULT '0',
  `input_data` text NOT NULL,
  `result_data` mediumtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.survey_panel_simulate_potential
CREATE TABLE IF NOT EXISTS `survey_panel_simulate_potential` (
  `id` tinytext NOT NULL,
  `result` text,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`(10))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.users_commands_calls
CREATE TABLE IF NOT EXISTS `users_commands_calls` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `command` tinytext,
  PRIMARY KEY (`id`),
  KEY `users_requests_user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8491 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.users_login_tokens
CREATE TABLE IF NOT EXISTS `users_login_tokens` (
  `id` int NOT NULL AUTO_INCREMENT,
  `guid` tinytext NOT NULL,
  `user_id` int NOT NULL,
  `ip_address` tinytext,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `expire_days` int NOT NULL DEFAULT '0',
  `firebase_token` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=512 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.users_password_reset_pending
CREATE TABLE IF NOT EXISTS `users_password_reset_pending` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` tinytext NOT NULL,
  `guid` tinytext NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.users_registration_pending
CREATE TABLE IF NOT EXISTS `users_registration_pending` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` tinytext NOT NULL,
  `password` varchar(100) NOT NULL,
  `secret` varbinary(100) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `guid` tinytext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_app_temp_data.users_requests
CREATE TABLE IF NOT EXISTS `users_requests` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `users_requests_user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3670 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_archive
CREATE DATABASE IF NOT EXISTS `gb_archive` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_archive`;

-- Dumping structure for table gb_archive.apps_concepts
CREATE TABLE IF NOT EXISTS `apps_concepts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `app_id_in_store` tinytext NOT NULL,
  `user_id` int NOT NULL,
  `platform` int NOT NULL,
  `store` int NOT NULL DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `title` tinytext,
  `description` mediumtext,
  `icon` tinytext,
  `copied_from_app_id_int` int DEFAULT NULL,
  `gallery` text,
  `concept_counter` int NOT NULL DEFAULT '0',
  `tagged_by_user` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `apps_concepts_app_id_int_index` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=61 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_archive.archive_data
CREATE TABLE IF NOT EXISTS `archive_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `expire_days` int NOT NULL DEFAULT '0',
  `type` tinytext NOT NULL,
  `data` mediumtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_archive.erik_steam_installs_fix
CREATE TABLE IF NOT EXISTS `erik_steam_installs_fix` (
  `id` int NOT NULL DEFAULT '0',
  `app_id_int` mediumint NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `score` tinyint NOT NULL,
  `installs` bigint NOT NULL,
  `ratings` int NOT NULL,
  `reviews` int NOT NULL,
  `changes` int NOT NULL DEFAULT '0',
  `price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',
  `initial_price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_archive.users_sent_emails
CREATE TABLE IF NOT EXISTS `users_sent_emails` (
  `id` int NOT NULL AUTO_INCREMENT,
  `guid` char(37) NOT NULL,
  `email` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `subject` tinytext NOT NULL,
  `body` text NOT NULL,
  `email_def` tinytext,
  `opened_t` timestamp NULL DEFAULT NULL,
  `from_composer` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `users_sent_emails_guid_uindex` (`guid`)
) ENGINE=InnoDB AUTO_INCREMENT=169 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_archive.users_tracking
CREATE TABLE IF NOT EXISTS `users_tracking` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `action` tinytext NOT NULL,
  `data` tinytext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=116 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_audit
CREATE DATABASE IF NOT EXISTS `gb_audit` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_audit`;

-- Dumping structure for table gb_audit.google_search
CREATE TABLE IF NOT EXISTS `google_search` (
  `id` int NOT NULL AUTO_INCREMENT,
  `audit_guid` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `context` tinytext,
  `q` tinytext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1827 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_audit.openai_chat_gpt
CREATE TABLE IF NOT EXISTS `openai_chat_gpt` (
  `id` int NOT NULL AUTO_INCREMENT,
  `audit_guid` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `tokens` int NOT NULL DEFAULT '0',
  `context` tinytext,
  `input_data` blob,
  `response_data` blob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=86148 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_audit.portal_admin
CREATE TABLE IF NOT EXISTS `portal_admin` (
  `id` int NOT NULL AUTO_INCREMENT,
  `audit_guid` tinytext NOT NULL,
  `user_id` int NOT NULL,
  `t` timestamp NULL DEFAULT NULL,
  `op` tinytext,
  `context` tinytext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_audit.users_credits
CREATE TABLE IF NOT EXISTS `users_credits` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `audit_guid` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `price` decimal(10,4) NOT NULL,
  `credits_before` decimal(10,4) NOT NULL,
  `context` tinytext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=87885 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_billing
CREATE DATABASE IF NOT EXISTS `gb_billing` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_billing`;

-- Dumping structure for table gb_billing.archive
CREATE TABLE IF NOT EXISTS `archive` (
  `billing_id` int NOT NULL,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `data` json NOT NULL,
  PRIMARY KEY (`user_id`,`billing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_billing.billings
CREATE TABLE IF NOT EXISTS `billings` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `organization_id` int NOT NULL,
  `guid` char(37) NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `activated_t` timestamp NOT NULL,
  `payment_id` int NOT NULL,
  `expire_t` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `billings_user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_billing.def_modules_discount
CREATE TABLE IF NOT EXISTS `def_modules_discount` (
  `id` int NOT NULL,
  `name` tinytext NOT NULL,
  `discount_perc` int NOT NULL,
  `invoice_name` tinytext NOT NULL,
  `invoice_desc` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_billing.def_modules_pricing
CREATE TABLE IF NOT EXISTS `def_modules_pricing` (
  `id` int NOT NULL,
  `name` tinytext NOT NULL,
  `price` decimal(8,2) NOT NULL,
  `hidden` int NOT NULL DEFAULT '0',
  `invoice_name` tinytext NOT NULL,
  `invoice_desc` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_billing.modules
CREATE TABLE IF NOT EXISTS `modules` (
  `billing_id` int NOT NULL,
  `module_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `seats` int NOT NULL,
  PRIMARY KEY (`billing_id`,`module_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_billing.requests
CREATE TABLE IF NOT EXISTS `requests` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `guid` char(37) NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `billing_details` json NOT NULL,
  `modules_config` json NOT NULL,
  `stripe_state` enum('request','cancel','success') NOT NULL DEFAULT 'request',
  `stripe_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `stripe_id` char(255) DEFAULT NULL,
  `stripe_subscription_id` tinytext,
  `test_payment` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `requests_user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_calc
CREATE DATABASE IF NOT EXISTS `gb_calc` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_calc`;

-- Dumping structure for table gb_calc.calc_tam_per_app_on_demand
CREATE TABLE IF NOT EXISTS `calc_tam_per_app_on_demand` (
  `app_id_int` int NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.requests
CREATE TABLE IF NOT EXISTS `requests` (
  `platform_id` int unsigned NOT NULL,
  `user_id` int unsigned NOT NULL,
  `conn_id` int unsigned NOT NULL,
  `hash_key` bigint NOT NULL,
  `t_start` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `t_end` timestamp NULL DEFAULT NULL,
  `t_heartbeat` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `calc` tinytext NOT NULL,
  `state` enum('queue','working','done','error','killed') NOT NULL,
  `input_data` text,
  `progress_data` tinytext,
  `survey_id` int unsigned NOT NULL,
  `version` int NOT NULL DEFAULT '0',
  `error_info` text,
  PRIMARY KEY (`platform_id`),
  KEY `requests_user_id_index` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.requests_app_recalc
CREATE TABLE IF NOT EXISTS `requests_app_recalc` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=141084 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.requests_queue
CREATE TABLE IF NOT EXISTS `requests_queue` (
  `platform_id` int unsigned NOT NULL,
  `user_id` int unsigned NOT NULL,
  `calc` tinytext NOT NULL,
  `hash_key` bigint NOT NULL,
  `survey_id` int unsigned NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `input_data` text,
  PRIMARY KEY (`platform_id`),
  KEY `requests_queue_user_id_index` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results__rejected_per_audience_angle
CREATE TABLE IF NOT EXISTS `results__rejected_per_audience_angle` (
  `platform_id` int unsigned NOT NULL,
  `audience_angle_id` bigint unsigned NOT NULL,
  `rejected_cnt` int unsigned NOT NULL,
  PRIMARY KEY (`platform_id`,`audience_angle_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results__ts_per_audience_angle
CREATE TABLE IF NOT EXISTS `results__ts_per_audience_angle` (
  `platform_id` int unsigned NOT NULL,
  `audience_angle_id` bigint unsigned NOT NULL,
  `ts` int unsigned NOT NULL,
  `no_data` tinyint NOT NULL DEFAULT '0',
  `competitors_count` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`audience_angle_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_affinities
CREATE TABLE IF NOT EXISTS `results_affinities` (
  `platform_id` int NOT NULL,
  `affinity_tag_id_int` int NOT NULL,
  `ts` int DEFAULT NULL,
  `audience_angle_id` int NOT NULL DEFAULT '0',
  `uuid` char(37) DEFAULT NULL,
  `competitors_cnt` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`affinity_tag_id_int`),
  UNIQUE KEY `results_affinities_uuid_uindex` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_audience_angle__input_tags
CREATE TABLE IF NOT EXISTS `results_audience_angle__input_tags` (
  `platform_id` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`tag_id_int`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_audience_angle__rejected
CREATE TABLE IF NOT EXISTS `results_audience_angle__rejected` (
  `platform_id` int unsigned NOT NULL,
  `audience_angle_id` int unsigned NOT NULL,
  `rejected_cnt` int unsigned NOT NULL,
  PRIMARY KEY (`platform_id`,`audience_angle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_audience_angles__competitors
CREATE TABLE IF NOT EXISTS `results_audience_angles__competitors` (
  `platform_id` int unsigned NOT NULL,
  `app_id_int` int NOT NULL,
  `audience_angle_id` int NOT NULL DEFAULT '0',
  `audience_angle_platform_id` int unsigned NOT NULL DEFAULT '0',
  `competitor_score` smallint NOT NULL,
  `threat_score` smallint unsigned NOT NULL,
  `similarity_w` smallint unsigned NOT NULL,
  `similarity_c` tinyint NOT NULL,
  `trend_w` smallint unsigned NOT NULL,
  `trend_c` tinyint NOT NULL,
  `quality_w` smallint unsigned NOT NULL,
  `quality_c` tinyint NOT NULL,
  `tam_w` smallint unsigned NOT NULL,
  `tam_c` tinyint NOT NULL,
  `size_w` smallint unsigned NOT NULL,
  `size_c` tinyint NOT NULL,
  `dev_id_int` int NOT NULL DEFAULT '0',
  `quality_portfolio` tinyint unsigned NOT NULL DEFAULT '0',
  `quality_full` int unsigned NOT NULL DEFAULT '0',
  `growth_w` smallint unsigned NOT NULL DEFAULT '0',
  `growth_c` tinyint NOT NULL DEFAULT '0',
  `tam_raw` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`app_id_int`,`audience_angle_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_audience_angles__final
CREATE TABLE IF NOT EXISTS `results_audience_angles__final` (
  `row_id` int NOT NULL AUTO_INCREMENT,
  `platform_id` int unsigned NOT NULL,
  `audience_angle_id` bigint unsigned NOT NULL,
  `age` smallint unsigned NOT NULL,
  `female` tinyint unsigned NOT NULL,
  `female_cnt` smallint unsigned NOT NULL,
  `ltv` smallint unsigned NOT NULL,
  `loved_cnt` smallint unsigned NOT NULL,
  `total_cnt` smallint unsigned NOT NULL,
  `rejected_cnt` smallint unsigned NOT NULL,
  `arpu` decimal(8,4) NOT NULL,
  `loyalty_installs` bigint unsigned NOT NULL,
  `rev_d` bigint unsigned NOT NULL,
  `exp_d` smallint unsigned NOT NULL,
  `qua_d` smallint unsigned NOT NULL,
  `potential_downloads` bigint NOT NULL DEFAULT '0',
  `total_audience` bigint unsigned NOT NULL DEFAULT '0',
  `loved_ratio_ext` decimal(8,4) NOT NULL DEFAULT '0.0000',
  `discount` int NOT NULL DEFAULT '0',
  `quality_full` int NOT NULL DEFAULT '0',
  `quality_portfolio` int NOT NULL DEFAULT '0',
  `dev_id_int` int NOT NULL DEFAULT '0',
  `rejected_ratio_ext` decimal(8,4) NOT NULL DEFAULT '0.0000',
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `results_audience_angles__uindex` (`platform_id`,`audience_angle_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=912 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_audience_angles__top_tags
CREATE TABLE IF NOT EXISTS `results_audience_angles__top_tags` (
  `platform_id` int unsigned NOT NULL,
  `audience_angle_id` bigint unsigned NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  `tag_order` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`audience_angle_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_competitors_cnt
CREATE TABLE IF NOT EXISTS `results_competitors_cnt` (
  `platform_id` int NOT NULL,
  `audience_angle_id` int unsigned NOT NULL,
  `competitors_cnt` int NOT NULL,
  PRIMARY KEY (`platform_id`,`audience_angle_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_gaps__nodes_ts
CREATE TABLE IF NOT EXISTS `results_gaps__nodes_ts` (
  `platform_id` bigint unsigned NOT NULL DEFAULT (0),
  `tag_id_int` bigint unsigned NOT NULL DEFAULT (0),
  `ts` int unsigned NOT NULL,
  PRIMARY KEY (`platform_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_gaps__opportunities
CREATE TABLE IF NOT EXISTS `results_gaps__opportunities` (
  `platform_id` bigint unsigned NOT NULL DEFAULT (0),
  `audience_angle_id` bigint unsigned NOT NULL DEFAULT (0),
  `affinity_tag_id_int` bigint unsigned NOT NULL DEFAULT (0),
  `age` tinyint unsigned NOT NULL,
  `female` tinyint unsigned NOT NULL,
  `female_cnt` smallint unsigned NOT NULL,
  `ltv` smallint unsigned NOT NULL,
  `potential_cnt` int unsigned DEFAULT NULL,
  `total_cnt` int unsigned DEFAULT NULL,
  `rejected_cnt` int unsigned NOT NULL,
  `threat_score` int DEFAULT NULL,
  `total_audience` bigint unsigned NOT NULL,
  `tam` bigint unsigned NOT NULL,
  `arpu` decimal(8,4) NOT NULL,
  `guid` char(37) NOT NULL,
  `potential_downloads` bigint NOT NULL DEFAULT '0',
  `loyalty_installs` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`platform_id`,`audience_angle_id`,`affinity_tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_gaps__opportunities_tags
CREATE TABLE IF NOT EXISTS `results_gaps__opportunities_tags` (
  `platform_id` bigint unsigned NOT NULL DEFAULT (0),
  `audience_angle_id` bigint unsigned NOT NULL DEFAULT (0),
  `tag_id_int` smallint unsigned NOT NULL,
  PRIMARY KEY (`platform_id`,`audience_angle_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.results_product_nodes__most_disliked
CREATE TABLE IF NOT EXISTS `results_product_nodes__most_disliked` (
  `platform_id` bigint unsigned NOT NULL DEFAULT (0),
  `tag_id_int` bigint unsigned NOT NULL DEFAULT (0),
  `tag_order` smallint unsigned NOT NULL,
  PRIMARY KEY (`platform_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_calc.tmp_part
CREATE TABLE IF NOT EXISTS `tmp_part` (
  `app_id_int` int unsigned NOT NULL,
  `tags_md5` binary(32) NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_dms
CREATE DATABASE IF NOT EXISTS `gb_dms` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_dms`;

-- Dumping structure for table gb_dms.dms
CREATE TABLE IF NOT EXISTS `dms` (
  `id` int NOT NULL,
  `dms_data` mediumblob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_platform
CREATE DATABASE IF NOT EXISTS `gb_platform` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_platform`;

-- Dumping structure for table gb_platform.audience_angle
CREATE TABLE IF NOT EXISTS `audience_angle` (
  `id` bigint unsigned NOT NULL,
  `angle_cnt` tinyint unsigned NOT NULL,
  `b` binary(48) NOT NULL,
  `platform_b` binary(48) DEFAULT NULL,
  `installs` bigint unsigned NOT NULL,
  `loyalty_installs` bigint unsigned NOT NULL,
  `arpu` decimal(8,4) NOT NULL,
  `valid_combination` int NOT NULL DEFAULT '0',
  `is_platform` int NOT NULL DEFAULT '0',
  `potential_downloads` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_potential
CREATE TABLE IF NOT EXISTS `audience_angle_potential` (
  `id` bigint unsigned NOT NULL,
  `survey_id` int unsigned NOT NULL,
  `loved_cnt` int unsigned NOT NULL DEFAULT '0',
  `total_cnt` int unsigned NOT NULL DEFAULT '0',
  `loved_ratio_ext` decimal(8,4) NOT NULL DEFAULT '0.0000',
  `rejected_ratio_ext` decimal(8,4) NOT NULL DEFAULT '0.0000',
  `loyalty_installs` bigint NOT NULL DEFAULT '0',
  `loved_hack` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`survey_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_rejected_per_app
CREATE TABLE IF NOT EXISTS `audience_angle_rejected_per_app` (
  `id` int NOT NULL,
  `survey_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `rejected_cnt` int NOT NULL,
  PRIMARY KEY (`id`,`survey_id`,`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_tags
CREATE TABLE IF NOT EXISTS `audience_angle_tags` (
  `id` bigint unsigned NOT NULL,
  `tag_id_int` int unsigned NOT NULL,
  PRIMARY KEY (`id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_tags_w
CREATE TABLE IF NOT EXISTS `audience_angle_tags_w` (
  `id` int unsigned NOT NULL,
  `survey_id` int unsigned NOT NULL,
  `w` int unsigned NOT NULL,
  `w_b` binary(48) NOT NULL,
  PRIMARY KEY (`id`,`survey_id`,`w`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_tam_per_angle
CREATE TABLE IF NOT EXISTS `audience_angle_tam_per_angle` (
  `id` int unsigned NOT NULL,
  `survey_id` int unsigned NOT NULL,
  `total_audience` bigint unsigned NOT NULL,
  `tam` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`,`survey_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_tam_per_app
CREATE TABLE IF NOT EXISTS `audience_angle_tam_per_app` (
  `app_id_int` int NOT NULL,
  `survey_id` int NOT NULL,
  `angle_cnt` int NOT NULL,
  `total_audience` bigint NOT NULL,
  `tam` bigint NOT NULL,
  `tags_md5` binary(32) NOT NULL,
  `max_angle_cnt` int NOT NULL,
  PRIMARY KEY (`survey_id`,`app_id_int`,`angle_cnt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.audience_angle_tam_per_app_state
CREATE TABLE IF NOT EXISTS `audience_angle_tam_per_app_state` (
  `app_id_int` int NOT NULL,
  `tags_md5` binary(32) NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.bin_bytes_cnt
CREATE TABLE IF NOT EXISTS `bin_bytes_cnt` (
  `cnt` int unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for function gb_platform.calc_discount
DELIMITER //
CREATE FUNCTION `calc_discount`(
	`yearly_revenues` bigint unsigned,
	`quality` int unsigned,
	`released_years` int unsigned
) RETURNS int unsigned
    DETERMINISTIC
BEGIN

    DECLARE revenues_ds int UNSIGNED;
    DECLARE quality_ds int UNSIGNED;
    DECLARE experience_ds int UNSIGNED;

    SET revenues_ds = 0;
    SET quality_ds = 0;
    SET experience_ds = 0;

    IF yearly_revenues > 0 THEN
        SET revenues_ds = (SELECT gb_platform.calc_discount_revenues(yearly_revenues));
    END IF;

    IF quality > 0 THEN
        SET quality_ds = (SELECT gb_platform.calc_discount_quality(quality));
    END IF;

    IF released_years > 0 THEN
        SET experience_ds = (SELECT gb_platform.calc_discount_experience(released_years));
    END IF;

    RETURN (revenues_ds + quality_ds + experience_ds);
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_discount_experience
DELIMITER //
CREATE FUNCTION `calc_discount_experience`(
	`released_years` int unsigned
) RETURNS int unsigned
    DETERMINISTIC
BEGIN

    DECLARE experience_ds int UNSIGNED;

    SET experience_ds = 0;

    IF released_years > 0 THEN
        SET experience_ds = ROUND(LEAST(released_years / 10 * 0.1, 0.1) * 100);
    END IF;

    RETURN experience_ds;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_discount_quality
DELIMITER //
CREATE FUNCTION `calc_discount_quality`(
	`quality` int unsigned
) RETURNS int unsigned
    DETERMINISTIC
BEGIN

    DECLARE quality_ds int UNSIGNED;

    SET quality_ds = 0;

    IF quality > 0 THEN
        SET quality_ds = ROUND(LEAST(quality / 100 * 0.1, 0.1) * 100);
    END IF;

    RETURN quality_ds;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_discount_revenues
DELIMITER //
CREATE FUNCTION `calc_discount_revenues`(
	`yearly_revenues` bigint unsigned
) RETURNS int unsigned
    DETERMINISTIC
BEGIN

    DECLARE revenues_ds int UNSIGNED;

    SET revenues_ds = 0;

    IF yearly_revenues > 0 THEN
        SET revenues_ds = ROUND(LEAST(yearly_revenues / (100 * 1000 * 1000) * 0.2, 0.2) * 100);
    END IF;

    RETURN revenues_ds;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_loyalty_installs
DELIMITER //
CREATE FUNCTION `calc_loyalty_installs`(
	`installs` BIGINT UNSIGNED,
	`months` SMALLINT UNSIGNED,
	`retention` DECIMAL(10, 4)
) RETURNS int
    DETERMINISTIC
BEGIN
    RETURN round(installs * 0.0075);
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_tam
DELIMITER //
CREATE FUNCTION `calc_tam`(
	`total_audience` bigint,
	`arpu` decimal(8, 4)
) RETURNS bigint
    DETERMINISTIC
BEGIN
    RETURN round(total_audience * arpu);
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_total_audience
DELIMITER //
CREATE FUNCTION `calc_total_audience`(
	`loved_survey_cnt` int unsigned,
	`total_survey_cnt` int unsigned,
	`rejected_survey_cnt` int unsigned,
	`potential_downloads` bigint unsigned,
	`loyalty_installs` bigint unsigned,
	`loved_ratio_ext` DECIMAL(8,4),
	`rejected_ratio_ext` DECIMAL(8,4)
) RETURNS bigint
    DETERMINISTIC
BEGIN

    DECLARE total_audience BIGINT UNSIGNED;
    DECLARE loved_absolute BIGINT UNSIGNED;
    DECLARE rejected_absolute BIGINT UNSIGNED;
    DECLARE loyal_absolute BIGINT UNSIGNED;

    DECLARE loved_survey_final BIGINT UNSIGNED;
    DECLARE rejected_survey_final BIGINT UNSIGNED;

    DECLARE rejected_survey_cnt_hack INT UNSIGNED;

    SET loved_survey_final = loved_survey_cnt;
    IF loved_ratio_ext != 0 THEN
        SET loved_survey_final = FLOOR(loved_ratio_ext * total_survey_cnt);
    END IF;

    SET rejected_survey_cnt_hack = rejected_survey_cnt;
    IF rejected_ratio_ext != 0 THEN
        SET rejected_survey_cnt_hack = FLOOR(rejected_ratio_ext * rejected_survey_cnt);
    end if;

    # RR = RSR * ( 1 + SIN ( RSR / 2 ) * 0.25 )
    SET rejected_survey_cnt_hack = FLOOR(rejected_survey_cnt_hack * (1 + SIN(rejected_survey_cnt_hack / 2) * 0.25));

    SET rejected_survey_final = rejected_survey_cnt_hack;

    SET loved_absolute = 0;
    SET rejected_absolute = 0;
    SET loyal_absolute = loyalty_installs;
    SET total_audience = 0;

    IF total_survey_cnt > 0 THEN
        SET loved_absolute = (loved_survey_final / total_survey_cnt) * potential_downloads;
    END IF;

    IF loved_survey_final > 0 THEN
        SET rejected_absolute = loved_absolute * (rejected_survey_final / loved_survey_final);
    END IF;

    IF loved_absolute > rejected_absolute + loyal_absolute THEN
        SET total_audience = loved_absolute - (rejected_absolute + loyal_absolute);
    END IF;

    RETURN total_audience;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.calc_ts_final
DELIMITER //
CREATE FUNCTION `calc_ts_final`(
	`ts` int,
	`discount` int
) RETURNS int
    DETERMINISTIC
BEGIN
    DECLARE ts_final int;

    SET ts_final = ts - discount;

    IF ts_final < 0 THEN
        SET ts_final = 0;
    end if;

    IF ts_final > 100 THEN
        SET ts_final = 100;
    end if;

    RETURN ts_final;
END//
DELIMITER ;

-- Dumping structure for table gb_platform.def_potential_downloads_bin
CREATE TABLE IF NOT EXISTS `def_potential_downloads_bin` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tags_b` binary(48) NOT NULL,
  `potential_downloads` bigint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.def_tags
CREATE TABLE IF NOT EXISTS `def_tags` (
  `tag_id_int` int unsigned NOT NULL,
  `is_survey` tinyint unsigned NOT NULL,
  `subcategory_int` int unsigned NOT NULL,
  `competitors_pool_w` int unsigned NOT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.def_tags_bin
CREATE TABLE IF NOT EXISTS `def_tags_bin` (
  `tag_id_int` smallint unsigned NOT NULL,
  `b` binary(48) NOT NULL,
  `not_rejected` int NOT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.def_tags_subc_bin
CREATE TABLE IF NOT EXISTS `def_tags_subc_bin` (
  `subcategory_int` smallint unsigned NOT NULL,
  `b` binary(48) NOT NULL,
  `competitors_pool_w` smallint unsigned NOT NULL,
  PRIMARY KEY (`subcategory_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.ext_arpu_per_tag
CREATE TABLE IF NOT EXISTS `ext_arpu_per_tag` (
  `tag_id_int` int NOT NULL,
  `arpu` decimal(8,4) DEFAULT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.ext_loved_ratio
CREATE TABLE IF NOT EXISTS `ext_loved_ratio` (
  `tag_id_int` int NOT NULL,
  `loved_ratio` decimal(8,4) DEFAULT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for function gb_platform.get_loyalty_installs
DELIMITER //
CREATE FUNCTION `get_loyalty_installs`(
	`loyalty_installs` bigint unsigned,
	`potential_downloads` bigint unsigned
) RETURNS bigint unsigned
    DETERMINISTIC
BEGIN
            DECLARE loyalty_installs_final bigint unsigned;
            SET loyalty_installs_final = FLOOR(loyalty_installs * gb_platform_proc.get_potential_downloads_ratio(potential_downloads));
            RETURN loyalty_installs_final;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.get_potential_downloads
DELIMITER //
CREATE FUNCTION `get_potential_downloads`(
	`tags_b` binary(48),
	`audience_angle_id` int
) RETURNS bigint unsigned
    DETERMINISTIC
BEGIN

            DECLARE potential_downloads bigint UNSIGNED;

            SET potential_downloads = NULL;

            IF audience_angle_id IS NOT NULL THEN
               SELECT aa.potential_downloads INTO potential_downloads
                 FROM gb_platform.audience_angle aa
                WHERE aa.id = audience_angle_id
                  AND aa.is_platform = 1;
            END IF;

            IF potential_downloads IS NULL AND tags_b IS NOT NULL THEN
                SELECT z1.potential_downloads INTO potential_downloads
                FROM (
                SELECT SUM(d.potential_downloads) as potential_downloads
                  FROM gb_platform.def_potential_downloads_bin d
                 WHERE BIT_COUNT(d.tags_b & tags_b) > 0 ) z1;
            END IF; 

            IF potential_downloads IS NULL OR potential_downloads = 0 THEN
               SET potential_downloads = 4386000000;
            END IF;

            RETURN potential_downloads;
END//
DELIMITER ;

-- Dumping structure for function gb_platform.get_potential_downloads_ratio
DELIMITER //
CREATE FUNCTION `get_potential_downloads_ratio`(
	`potential_downloads` bigint unsigned
) RETURNS decimal(5,4)
    DETERMINISTIC
BEGIN
            DECLARE potential_downloads_ratio decimal(5, 4);
            SET potential_downloads_ratio = ROUND(potential_downloads / 4386000000, 4);
            RETURN potential_downloads_ratio;
END//
DELIMITER ;

-- Dumping structure for table gb_platform.id_sequencer
CREATE TABLE IF NOT EXISTS `id_sequencer` (
  `id` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for function gb_platform.next_platform_id
DELIMITER //
CREATE FUNCTION `next_platform_id`() RETURNS int
    DETERMINISTIC
BEGIN
    SET @res = 0;
    UPDATE gb_platform.id_sequencer SET id=LAST_INSERT_ID(id+1);
    SELECT LAST_INSERT_ID() INTO @res;
    RETURN @res;
END//
DELIMITER ;

-- Dumping structure for table gb_platform.platform_values_apps
CREATE TABLE IF NOT EXISTS `platform_values_apps` (
  `app_id_int` int NOT NULL,
  `installs` bigint NOT NULL,
  `loyalty_installs` bigint NOT NULL,
  `rating` tinyint NOT NULL,
  `store` tinyint NOT NULL,
  `tier` tinyint NOT NULL,
  `released_years` tinyint NOT NULL,
  `d28_ret_gp` decimal(8,4) NOT NULL,
  `downloads_12m_gp_us` bigint NOT NULL,
  `downloads_12m_gp_ww` bigint NOT NULL,
  `downloads_gp_us` bigint NOT NULL,
  `downloads_gp_ww` bigint NOT NULL,
  `revenues_12m_gp_us` bigint NOT NULL,
  `revenues_12m_gp_ww` bigint NOT NULL,
  `revenues_gp_us` bigint NOT NULL,
  `revenues_gp_ww` bigint NOT NULL,
  `growth` bigint NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_apps__single
CREATE TABLE IF NOT EXISTS `platform_values_apps__single` (
  `app_id_int` int NOT NULL,
  `installs` bigint NOT NULL,
  `loyalty_installs` bigint NOT NULL,
  `rating` tinyint NOT NULL,
  `store` tinyint NOT NULL,
  `tier` tinyint NOT NULL,
  `released_years` tinyint NOT NULL,
  `d28_ret_gp` decimal(8,4) NOT NULL,
  `downloads_12m_gp_us` bigint NOT NULL,
  `downloads_12m_gp_ww` bigint NOT NULL,
  `downloads_gp_us` bigint NOT NULL,
  `downloads_gp_ww` bigint NOT NULL,
  `revenues_12m_gp_us` bigint NOT NULL,
  `revenues_12m_gp_ww` bigint NOT NULL,
  `revenues_gp_us` bigint NOT NULL,
  `revenues_gp_ww` bigint NOT NULL,
  `growth` bigint NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_apps_tags_per_subc
CREATE TABLE IF NOT EXISTS `platform_values_apps_tags_per_subc` (
  `app_id_int` mediumint NOT NULL,
  `subcategory_int` smallint unsigned NOT NULL,
  `cnt` smallint unsigned NOT NULL,
  PRIMARY KEY (`app_id_int`,`subcategory_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_devs
CREATE TABLE IF NOT EXISTS `platform_values_devs` (
  `dev_id_int` int unsigned NOT NULL,
  `yearly_installs` bigint unsigned NOT NULL,
  `arpu` decimal(8,4) NOT NULL,
  `yearly_revenues` bigint unsigned NOT NULL,
  PRIMARY KEY (`dev_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_devs_apps
CREATE TABLE IF NOT EXISTS `platform_values_devs_apps` (
  `dev_id_int` mediumint NOT NULL,
  `app_id_int` mediumint NOT NULL,
  PRIMARY KEY (`dev_id_int`,`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_installs_arpu_per_tag
CREATE TABLE IF NOT EXISTS `platform_values_installs_arpu_per_tag` (
  `tag_id_int` smallint unsigned NOT NULL,
  `installs` bigint unsigned NOT NULL,
  `loyalty_installs` bigint unsigned NOT NULL,
  `arpu` decimal(8,4) NOT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_competitors
CREATE TABLE IF NOT EXISTS `platform_values_survey_competitors` (
  `survey_meta_id` smallint unsigned NOT NULL,
  `survey_instance_int` mediumint unsigned NOT NULL,
  `app_id_int` int unsigned NOT NULL,
  `tag_value` int unsigned NOT NULL,
  PRIMARY KEY (`survey_meta_id`,`survey_instance_int`,`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_data_per_tag
CREATE TABLE IF NOT EXISTS `platform_values_survey_data_per_tag` (
  `survey_id` int unsigned NOT NULL,
  `tag_id_int` int unsigned NOT NULL,
  `filtered_rows` int unsigned NOT NULL,
  `total_rows` int unsigned NOT NULL,
  `tam` bigint unsigned NOT NULL,
  PRIMARY KEY (`survey_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_flags
CREATE TABLE IF NOT EXISTS `platform_values_survey_flags` (
  `survey_id` smallint unsigned NOT NULL,
  `survey_instance_int` mediumint unsigned NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  PRIMARY KEY (`survey_id`,`survey_instance_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_info
CREATE TABLE IF NOT EXISTS `platform_values_survey_info` (
  `survey_meta_id` smallint unsigned NOT NULL,
  `survey_instance_int` mediumint unsigned NOT NULL,
  `age` smallint unsigned NOT NULL,
  `female` tinyint unsigned NOT NULL,
  `spending` smallint unsigned NOT NULL,
  `favorite_app_id_int` int unsigned NOT NULL,
  PRIMARY KEY (`survey_meta_id`,`survey_instance_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_tags
CREATE TABLE IF NOT EXISTS `platform_values_survey_tags` (
  `survey_meta_id` smallint unsigned NOT NULL,
  `survey_instance_int` mediumint unsigned NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  `tag_value` tinyint unsigned NOT NULL,
  `loved` tinyint unsigned NOT NULL,
  `hated` tinyint unsigned NOT NULL,
  `rejected` tinyint unsigned NOT NULL,
  PRIMARY KEY (`survey_meta_id`,`survey_instance_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_survey_tags_bin
CREATE TABLE IF NOT EXISTS `platform_values_survey_tags_bin` (
  `survey_id` smallint NOT NULL,
  `survey_instance_int` int unsigned NOT NULL,
  `b` binary(48) NOT NULL,
  `b_hated` binary(48) NOT NULL,
  `b_loved` binary(48) NOT NULL,
  `b_rejected` binary(48) NOT NULL,
  PRIMARY KEY (`survey_id`,`survey_instance_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags
CREATE TABLE IF NOT EXISTS `platform_values_tags` (
  `app_id_int` mediumint NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  `tag_rank` tinyint unsigned NOT NULL,
  PRIMARY KEY (`app_id_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_arpu
CREATE TABLE IF NOT EXISTS `platform_values_tags_arpu` (
  `tag_id_int` smallint unsigned NOT NULL,
  `arpu_ww` decimal(12,4) NOT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_bin
CREATE TABLE IF NOT EXISTS `platform_values_tags_bin` (
  `app_id_int` mediumint NOT NULL,
  `tags_md5` binary(32) NOT NULL,
  `b` binary(48) NOT NULL,
  `ranked_genres_b` binary(48) NOT NULL,
  `subgenres_b` binary(48) NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_fake_concepts
CREATE TABLE IF NOT EXISTS `platform_values_tags_fake_concepts` (
  `survey_id` int unsigned NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  `fake_concept_id` smallint unsigned NOT NULL,
  `fake_concept_tag_id_int` smallint unsigned NOT NULL,
  PRIMARY KEY (`survey_id`,`tag_id_int`,`fake_concept_tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_fake_concepts_bin
CREATE TABLE IF NOT EXISTS `platform_values_tags_fake_concepts_bin` (
  `survey_id` int unsigned NOT NULL,
  `tag_id_int` int unsigned NOT NULL,
  `b` binary(48) NOT NULL,
  PRIMARY KEY (`survey_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_fake_concepts_tam
CREATE TABLE IF NOT EXISTS `platform_values_tags_fake_concepts_tam` (
  `tag_id_int` int unsigned NOT NULL,
  `survey_id` int unsigned NOT NULL,
  `tam` bigint unsigned NOT NULL,
  PRIMARY KEY (`survey_id`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_model
CREATE TABLE IF NOT EXISTS `platform_values_tags_model` (
  `tag_id_int` smallint unsigned NOT NULL,
  `d28_ret_gp` decimal(18,8) NOT NULL,
  `downloads_12m_gp_us` decimal(18,8) NOT NULL,
  `downloads_12m_gp_ww` decimal(18,8) NOT NULL,
  `downloads_gp_us` decimal(18,8) NOT NULL,
  `downloads_gp_ww` decimal(18,8) NOT NULL,
  `revenues_12m_gp_us` decimal(18,8) NOT NULL,
  `revenues_12m_gp_ww` decimal(18,8) NOT NULL,
  `revenues_gp_us` decimal(18,8) NOT NULL,
  `revenues_gp_ww` decimal(18,8) NOT NULL,
  PRIMARY KEY (`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_model_intercept
CREATE TABLE IF NOT EXISTS `platform_values_tags_model_intercept` (
  `d28_ret_gp` decimal(18,8) NOT NULL,
  `downloads_12m_gp_us` decimal(18,8) NOT NULL,
  `downloads_12m_gp_ww` decimal(18,8) NOT NULL,
  `downloads_gp_us` decimal(18,8) NOT NULL,
  `downloads_gp_ww` decimal(18,8) NOT NULL,
  `revenues_12m_gp_us` decimal(18,8) NOT NULL,
  `revenues_12m_gp_ww` decimal(18,8) NOT NULL,
  `revenues_gp_us` decimal(18,8) NOT NULL,
  `revenues_gp_ww` decimal(18,8) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_platform
CREATE TABLE IF NOT EXISTS `platform_values_tags_platform` (
  `app_id_int` mediumint NOT NULL,
  `tag_id_int` smallint unsigned NOT NULL,
  PRIMARY KEY (`app_id_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.platform_values_tags_platform_bin
CREATE TABLE IF NOT EXISTS `platform_values_tags_platform_bin` (
  `app_id_int` mediumint NOT NULL,
  `b` binary(48) NOT NULL,
  `potential_downloads` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_platform.zero_bin_value
CREATE TABLE IF NOT EXISTS `zero_bin_value` (
  `b` binary(48) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_public
CREATE DATABASE IF NOT EXISTS `gb_public` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_public`;

-- Dumping structure for table gb_public.email_subscription
CREATE TABLE IF NOT EXISTS `email_subscription` (
  `email_id` int unsigned NOT NULL,
  `subscribed` int unsigned NOT NULL,
  `request_guid` char(37) DEFAULT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  PRIMARY KEY (`email_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_public.emails
CREATE TABLE IF NOT EXISTS `emails` (
  `email_id` int unsigned NOT NULL AUTO_INCREMENT,
  `email` tinytext NOT NULL,
  PRIMARY KEY (`email_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_public.registration_whitelist_pending
CREATE TABLE IF NOT EXISTS `registration_whitelist_pending` (
  `email_id` int unsigned NOT NULL DEFAULT (0),
  `t` timestamp NOT NULL DEFAULT (now()),
  `ip_address` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `request_guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `confirmed` int NOT NULL DEFAULT '0',
  `responded_t` timestamp NULL DEFAULT NULL,
  `blocked` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`email_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.


-- Dumping database structure for gb_scraped_data
CREATE DATABASE IF NOT EXISTS `gb_scraped_data` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_scraped_data`;

-- Dumping structure for table gb_scraped_data.apps
CREATE TABLE IF NOT EXISTS `apps` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `app_id_in_store` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `title` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `released` timestamp NULL DEFAULT NULL,
  `platform` int NOT NULL,
  `store` int NOT NULL,
  `icon` tinytext,
  `description` mediumtext,
  `rank_val` bigint NOT NULL DEFAULT (0),
  `genres` tinytext,
  `rating` int NOT NULL DEFAULT '0',
  `installs` bigint NOT NULL DEFAULT '0',
  `gallery` text,
  `loyalty_installs` bigint NOT NULL DEFAULT '0',
  `loyalty_installs_t` timestamp NULL DEFAULT NULL,
  `scraped_t` timestamp NULL DEFAULT NULL,
  `scraped_request` timestamp NULL DEFAULT NULL,
  `ratings` int NOT NULL DEFAULT '0',
  `reviews` int NOT NULL DEFAULT '0',
  `price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',
  `initial_price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',
  `url` tinytext,
  `removed_from_store` timestamp NULL DEFAULT NULL,
  `ads` int NOT NULL DEFAULT '0',
  `iap` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `apps_app_id_int_uindex` (`app_id_int`),
  CONSTRAINT `app_id_int_not_zero` CHECK ((`app_id_int` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=1058465 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_concepts
CREATE TABLE IF NOT EXISTS `apps_concepts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `app_id_in_store` tinytext NOT NULL,
  `user_id` int NOT NULL,
  `platform` int NOT NULL,
  `store` int NOT NULL DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `title` tinytext,
  `description` mediumtext,
  `icon` tinytext,
  `copied_from_app_id_int` int DEFAULT NULL,
  `gallery` text,
  `concept_counter` int NOT NULL DEFAULT '0',
  `tagged_by_user` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `apps_concepts_app_id_int_index` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=70 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_gallery
CREATE TABLE IF NOT EXISTS `apps_gallery` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `app_id_int` int unsigned NOT NULL,
  `img_order` tinyint unsigned NOT NULL DEFAULT '0',
  `dms_id` int unsigned NOT NULL DEFAULT '0',
  `store_url` tinytext,
  PRIMARY KEY (`id`),
  KEY `apps_gallery_app_id_int_index` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=11947807 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_hist
CREATE TABLE IF NOT EXISTS `apps_hist` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` mediumint NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `score` tinyint NOT NULL,
  `installs` bigint NOT NULL,
  `ratings` int NOT NULL,
  `reviews` int NOT NULL,
  `changes` int NOT NULL DEFAULT '0',
  `price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',
  `initial_price` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=959077 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_icons
CREATE TABLE IF NOT EXISTS `apps_icons` (
  `app_id_int` int NOT NULL,
  `icon` longblob NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_source_data
CREATE TABLE IF NOT EXISTS `apps_source_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id` tinytext,
  `app_id_int` int NOT NULL,
  `data_type` tinytext NOT NULL,
  `data` mediumblob NOT NULL,
  PRIMARY KEY (`id`),
  KEY `apps_store_page_app_id_index` (`app_id`(10)),
  KEY `apps_store_page_app_id_int_index` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=1552311 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_store_tags
CREATE TABLE IF NOT EXISTS `apps_store_tags` (
  `app_id_int` int NOT NULL,
  `store_tag_id` int NOT NULL,
  PRIMARY KEY (`app_id_int`,`store_tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.apps_to_scrap_batch
CREATE TABLE IF NOT EXISTS `apps_to_scrap_batch` (
  `app_id_in_store` tinytext NOT NULL,
  `store` int DEFAULT NULL,
  `state` tinytext,
  PRIMARY KEY (`app_id_in_store`(50))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for view gb_scraped_data.apps_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `apps_v` (
	`id` INT NOT NULL,
	`app_id_int` INT NOT NULL,
	`app_id_in_store` TINYTEXT NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`title` TINYTEXT NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`t` TIMESTAMP NOT NULL,
	`released` TIMESTAMP NULL,
	`platform` INT NOT NULL,
	`store` INT NOT NULL,
	`icon` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`description` MEDIUMTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`rank_val` BIGINT NOT NULL,
	`genres` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`rating` INT NOT NULL,
	`installs` BIGINT NOT NULL,
	`gallery` TEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`loyalty_installs` BIGINT NOT NULL,
	`loyalty_installs_t` TIMESTAMP NULL,
	`scraped_t` TIMESTAMP NULL,
	`scraped_request` TIMESTAMP NULL,
	`ratings` INT NOT NULL,
	`reviews` INT NOT NULL,
	`price` DECIMAL(8,2) UNSIGNED NOT NULL,
	`initial_price` DECIMAL(8,2) UNSIGNED NOT NULL,
	`url` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`removed_from_store` TIMESTAMP NULL
);

-- Dumping structure for view gb_scraped_data.apps_valid
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `apps_valid` (
	`id` INT NOT NULL,
	`app_id_int` INT NOT NULL,
	`app_id_in_store` TINYTEXT NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`title` TINYTEXT NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`t` TIMESTAMP NOT NULL,
	`released` TIMESTAMP NULL,
	`platform` INT NOT NULL,
	`store` INT NOT NULL,
	`icon` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`description` MEDIUMTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`rank_val` BIGINT NOT NULL,
	`genres` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`rating` INT NOT NULL,
	`installs` BIGINT NOT NULL,
	`gallery` TEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`loyalty_installs` BIGINT NOT NULL,
	`loyalty_installs_t` TIMESTAMP NULL,
	`scraped_t` TIMESTAMP NULL,
	`scraped_request` TIMESTAMP NULL,
	`ratings` INT NOT NULL,
	`reviews` INT NOT NULL,
	`price` DECIMAL(8,2) UNSIGNED NOT NULL,
	`initial_price` DECIMAL(8,2) UNSIGNED NOT NULL,
	`url` TINYTEXT NULL COLLATE 'utf8mb4_0900_ai_ci',
	`removed_from_store` TIMESTAMP NULL
);

-- Dumping structure for table gb_scraped_data.def_map_store_tags
CREATE TABLE IF NOT EXISTS `def_map_store_tags` (
  `tag_id_int` int NOT NULL,
  `store_tag_id` int NOT NULL,
  `type` enum('override','append') NOT NULL,
  PRIMARY KEY (`store_tag_id`,`tag_id_int`),
  UNIQUE KEY `def_map_store_tags_tag_id_int_store_tag_id_uindex` (`tag_id_int`,`store_tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.def_store_tags
CREATE TABLE IF NOT EXISTS `def_store_tags` (
  `id` int NOT NULL AUTO_INCREMENT,
  `store` int NOT NULL,
  `store_tag` tinytext NOT NULL,
  `store_tag_raw` tinytext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `def_store_tags_store_tag_uindex` (`store_tag`(50))
) ENGINE=InnoDB AUTO_INCREMENT=1126 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs
CREATE TABLE IF NOT EXISTS `devs` (
  `dev_id_int` int NOT NULL,
  `dev_id_in_store` tinytext NOT NULL,
  `title` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `store` int NOT NULL,
  `scraped_t` timestamp NULL DEFAULT NULL,
  `url` tinytext,
  `scrap_request_t` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`dev_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_apps
CREATE TABLE IF NOT EXISTS `devs_apps` (
  `dev_id_int` int NOT NULL,
  `app_id_int` int NOT NULL DEFAULT '0',
  `primary_dev` int unsigned NOT NULL DEFAULT '1',
  PRIMARY KEY (`dev_id_int`,`app_id_int`) USING BTREE,
  KEY `devs_apps_app_id_int_index` (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_concepts
CREATE TABLE IF NOT EXISTS `devs_concepts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dev_id_int` int NOT NULL,
  `title` tinytext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `devs_concepts_dev_id_int_index` (`dev_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_concepts_counter
CREATE TABLE IF NOT EXISTS `devs_concepts_counter` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dev_id` tinytext NOT NULL,
  `counter` int DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `devs_concepts_counter_dev_id_index` (`dev_id`(10))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_concepts_users
CREATE TABLE IF NOT EXISTS `devs_concepts_users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dev_id` tinytext,
  `dev_id_int` int NOT NULL,
  `user_id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_devs
CREATE TABLE IF NOT EXISTS `devs_devs` (
  `parent_dev_id_int` int NOT NULL,
  `child_dev_id_int` int NOT NULL,
  PRIMARY KEY (`parent_dev_id_int`,`child_dev_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_scraped_data.devs_source_data
CREATE TABLE IF NOT EXISTS `devs_source_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dev_id` tinytext,
  `dev_id_int` int NOT NULL,
  `data_type` tinytext NOT NULL,
  `data` mediumblob NOT NULL,
  PRIMARY KEY (`id`),
  KEY `devs_store_page_dev_id_index` (`dev_id`(10)),
  KEY `devs_source_data_dev_id_int_index` (`dev_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=138400 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for function gb_scraped_data.get_app_growth
DELIMITER //
CREATE FUNCTION `get_app_growth`(
	`app_id_int_input` int unsigned
) RETURNS bigint
    DETERMINISTIC
BEGIN

    DECLARE app_growth bigint;

    SET app_growth = (
        SELECT IF(p.app_id_int IS NULL, (IF(TIMESTAMPDIFF(YEAR, a.released, CURDATE()) = 0, a.installs, ROUND(a.installs / TIMESTAMPDIFF(YEAR, a.released, CURDATE())))), p.growth) as growth
          FROM gb_scraped_data.apps a
          LEFT JOIN gb_platform.platform_values_apps p ON p.app_id_int = a.app_id_int
         WHERE a.app_id_int = app_id_int_input
        );

    RETURN app_growth;
END//
DELIMITER ;

-- Dumping structure for function gb_scraped_data.get_app_tier
DELIMITER //
CREATE FUNCTION `get_app_tier`(
	`app_id_int` int unsigned
) RETURNS int
    DETERMINISTIC
BEGIN

    DECLARE app_tier int UNSIGNED;

    SET app_tier = (
        SELECT t.tier
          FROM gb_app.def_sheet_platform_values_install_tiers t,
               gb_scraped_data.apps a
         WHERE t.store_id = a.store
           AND a.installs >= t.value_from
           AND a.installs < t.value_to
           AND a.app_id_int = app_id_int
        );

    RETURN app_tier;
END//
DELIMITER ;

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `apps_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `gb_scraped_data`.`apps_v` AS select `gb_scraped_data`.`apps`.`id` AS `id`,`gb_scraped_data`.`apps`.`app_id_int` AS `app_id_int`,`gb_scraped_data`.`apps`.`app_id_in_store` AS `app_id_in_store`,`gb_scraped_data`.`apps`.`title` AS `title`,`gb_scraped_data`.`apps`.`t` AS `t`,`gb_scraped_data`.`apps`.`released` AS `released`,`gb_scraped_data`.`apps`.`platform` AS `platform`,`gb_scraped_data`.`apps`.`store` AS `store`,`gb_scraped_data`.`apps`.`icon` AS `icon`,`gb_scraped_data`.`apps`.`description` AS `description`,`gb_scraped_data`.`apps`.`rank_val` AS `rank_val`,`gb_scraped_data`.`apps`.`genres` AS `genres`,`gb_scraped_data`.`apps`.`rating` AS `rating`,`gb_scraped_data`.`apps`.`installs` AS `installs`,`gb_scraped_data`.`apps`.`gallery` AS `gallery`,`gb_scraped_data`.`apps`.`loyalty_installs` AS `loyalty_installs`,`gb_scraped_data`.`apps`.`loyalty_installs_t` AS `loyalty_installs_t`,`gb_scraped_data`.`apps`.`scraped_t` AS `scraped_t`,`gb_scraped_data`.`apps`.`scraped_request` AS `scraped_request`,`gb_scraped_data`.`apps`.`ratings` AS `ratings`,`gb_scraped_data`.`apps`.`reviews` AS `reviews`,`gb_scraped_data`.`apps`.`price` AS `price`,`gb_scraped_data`.`apps`.`initial_price` AS `initial_price`,`gb_scraped_data`.`apps`.`url` AS `url`,`gb_scraped_data`.`apps`.`removed_from_store` AS `removed_from_store` from `gb_scraped_data`.`apps` where (`gb_scraped_data`.`apps`.`removed_from_store` is null)
;

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `apps_valid`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `gb_scraped_data`.`apps_valid` AS select `gb_scraped_data`.`apps`.`id` AS `id`,`gb_scraped_data`.`apps`.`app_id_int` AS `app_id_int`,`gb_scraped_data`.`apps`.`app_id_in_store` AS `app_id_in_store`,`gb_scraped_data`.`apps`.`title` AS `title`,`gb_scraped_data`.`apps`.`t` AS `t`,`gb_scraped_data`.`apps`.`released` AS `released`,`gb_scraped_data`.`apps`.`platform` AS `platform`,`gb_scraped_data`.`apps`.`store` AS `store`,`gb_scraped_data`.`apps`.`icon` AS `icon`,`gb_scraped_data`.`apps`.`description` AS `description`,`gb_scraped_data`.`apps`.`rank_val` AS `rank_val`,`gb_scraped_data`.`apps`.`genres` AS `genres`,`gb_scraped_data`.`apps`.`rating` AS `rating`,`gb_scraped_data`.`apps`.`installs` AS `installs`,`gb_scraped_data`.`apps`.`gallery` AS `gallery`,`gb_scraped_data`.`apps`.`loyalty_installs` AS `loyalty_installs`,`gb_scraped_data`.`apps`.`loyalty_installs_t` AS `loyalty_installs_t`,`gb_scraped_data`.`apps`.`scraped_t` AS `scraped_t`,`gb_scraped_data`.`apps`.`scraped_request` AS `scraped_request`,`gb_scraped_data`.`apps`.`ratings` AS `ratings`,`gb_scraped_data`.`apps`.`reviews` AS `reviews`,`gb_scraped_data`.`apps`.`price` AS `price`,`gb_scraped_data`.`apps`.`initial_price` AS `initial_price`,`gb_scraped_data`.`apps`.`url` AS `url`,`gb_scraped_data`.`apps`.`removed_from_store` AS `removed_from_store` from `gb_scraped_data`.`apps` where (`gb_scraped_data`.`apps`.`scraped_t` is not null)
;


-- Dumping database structure for gb_server_processes
CREATE DATABASE IF NOT EXISTS `gb_server_processes` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_server_processes`;

-- Dumping structure for table gb_server_processes.processes
CREATE TABLE IF NOT EXISTS `processes` (
  `pid` int NOT NULL,
  `created_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `heartbeat_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `state` enum('waiting','working','finished') NOT NULL,
  `is_error` int NOT NULL DEFAULT '0',
  `error_msg` tinytext,
  `process_name` tinytext,
  `os_info` tinytext,
  `force_kill` int NOT NULL DEFAULT '0',
  `input_args` text,
  PRIMARY KEY (`pid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_survey_data
CREATE DATABASE IF NOT EXISTS `gb_survey_data` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_survey_data`;

-- Dumping structure for table gb_survey_data.survey_competitors
CREATE TABLE IF NOT EXISTS `survey_competitors` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `survey_instance` tinytext NOT NULL,
  `app_id` tinytext NOT NULL,
  `value` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `survey_competitors_survey_instance_index` (`survey_instance`(10))
) ENGINE=InnoDB AUTO_INCREMENT=174634 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_flags
CREATE TABLE IF NOT EXISTS `survey_flags` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `survey_id` int unsigned NOT NULL,
  `survey_instance` tinytext NOT NULL,
  `tag_id` tinytext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `survey_flags_survey_id_survey_instance_tag_id_int_index` (`survey_id`,`survey_instance`(10),`tag_id`(10))
) ENGINE=InnoDB AUTO_INCREMENT=61233 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_genres_dcm
CREATE TABLE IF NOT EXISTS `survey_genres_dcm` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `survey_instance` tinytext NOT NULL,
  `dcm_order` int NOT NULL,
  `genre_1` tinytext,
  `genre_2` tinytext,
  `core_1` tinytext,
  `core_2` tinytext,
  `chosen` tinytext,
  PRIMARY KEY (`id`),
  KEY `survey_genres_dcm_survey_instance_index` (`survey_instance`(10))
) ENGINE=InnoDB AUTO_INCREMENT=63156 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_info
CREATE TABLE IF NOT EXISTS `survey_info` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `survey_instance` tinytext NOT NULL,
  `age` int NOT NULL,
  `gender` tinytext NOT NULL,
  `role` int NOT NULL,
  `favorite_game` tinytext NOT NULL,
  `spending` int NOT NULL,
  `playing_time` int NOT NULL,
  `loyalty` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `survey_info_survey_instance_index` (`survey_instance`(10))
) ENGINE=InnoDB AUTO_INCREMENT=13899 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_meta
CREATE TABLE IF NOT EXISTS `survey_meta` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_control_guid` tinytext NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `version` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_tags
CREATE TABLE IF NOT EXISTS `survey_tags` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `tag_id` tinytext NOT NULL,
  `tag_value` int NOT NULL,
  `survey_instance` tinytext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `survey_tags_survey_instance_index` (`survey_instance`(10)),
  KEY `survey_tags_survey_meta_id_survey_instance_int_index` (`survey_meta_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2967297 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_tam_per_tag
CREATE TABLE IF NOT EXISTS `survey_tam_per_tag` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `ltv` int NOT NULL DEFAULT '0',
  `filtered_cnt` int NOT NULL DEFAULT '0',
  `total_cnt` int NOT NULL DEFAULT '0',
  `tam_base` decimal(12,8) NOT NULL DEFAULT '0.00000000',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1257 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_survey_data.survey_topics_dcm
CREATE TABLE IF NOT EXISTS `survey_topics_dcm` (
  `id` int NOT NULL AUTO_INCREMENT,
  `survey_meta_id` int NOT NULL,
  `survey_instance` tinytext,
  `dcm_order` int NOT NULL,
  `topic` tinytext NOT NULL,
  `feature_1a` tinytext NOT NULL,
  `feature_1b` tinytext NOT NULL,
  `feature_2a` tinytext NOT NULL,
  `feature_2b` tinytext NOT NULL,
  `chosen_a` tinytext,
  `chosen_b` tinytext,
  PRIMARY KEY (`id`),
  KEY `survey_topics_dcm_survey_instance_index` (`survey_instance`(10))
) ENGINE=InnoDB AUTO_INCREMENT=49381 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.


-- Dumping database structure for gb_tagged_data
CREATE DATABASE IF NOT EXISTS `gb_tagged_data` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_tagged_data`;

-- Dumping structure for table gb_tagged_data.active_prompts_b
CREATE TABLE IF NOT EXISTS `active_prompts_b` (
  `b` bigint unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.def_prompts
CREATE TABLE IF NOT EXISTS `def_prompts` (
  `id` int DEFAULT NULL,
  `prompt_id` tinytext,
  `order_by` int DEFAULT NULL,
  `active` int NOT NULL DEFAULT '0',
  `b` bigint unsigned NOT NULL DEFAULT '0',
  `rank_prompt` int NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.gpt_prompts
CREATE TABLE IF NOT EXISTS `gpt_prompts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `audit_guid` tinytext,
  `app_id` tinytext,
  `app_id_int` int NOT NULL,
  `prompt_id` tinytext NOT NULL,
  `user_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `finish_reason` tinytext NOT NULL,
  `tokens` int NOT NULL DEFAULT '0',
  `result` mediumtext,
  `parse_state` int NOT NULL DEFAULT '0',
  `parse_reason` tinytext,
  `retry_row_id` int NOT NULL DEFAULT '0',
  `retry_count` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=154505 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.gpt_unknown_ranked_tags
CREATE TABLE IF NOT EXISTS `gpt_unknown_ranked_tags` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `audit_guid` tinytext NOT NULL,
  `category` tinytext,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `node` tinytext,
  `tag_rank` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1359 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagged
CREATE TABLE IF NOT EXISTS `platform_tagged` (
  `app_id_int` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `prompts_b` bigint unsigned NOT NULL DEFAULT '0',
  `manual` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagged_history
CREATE TABLE IF NOT EXISTS `platform_tagged_history` (
  `hist_id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `hist_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `context` tinytext,
  PRIMARY KEY (`hist_id`,`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=19401 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagging_batch
CREATE TABLE IF NOT EXISTS `platform_tagging_batch` (
  `app_id_int` int NOT NULL,
  `only_priority_prompt` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagging_prompts
CREATE TABLE IF NOT EXISTS `platform_tagging_prompts` (
  `request_id` int NOT NULL,
  `prompt_id` varchar(50) NOT NULL,
  `state` enum('queue','working','retry','error','done') NOT NULL,
  `created_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `parsed_result` json DEFAULT NULL,
  `order_by` int NOT NULL DEFAULT '0',
  `prompt_row_id` int NOT NULL DEFAULT '0',
  `retry_prompt_row_id` int NOT NULL DEFAULT '0',
  `error_data` json DEFAULT NULL,
  PRIMARY KEY (`request_id`,`prompt_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagging_request
CREATE TABLE IF NOT EXISTS `platform_tagging_request` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `state` enum('queue','working','retry','error','done') NOT NULL,
  `request_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `progress` int NOT NULL DEFAULT '0',
  `next_retry_t` timestamp NULL DEFAULT NULL,
  `error_data` tinytext,
  `remove_only_prompts_tags` int NOT NULL DEFAULT '0',
  `from_batch` int NOT NULL DEFAULT '0',
  `only_priority_prompt` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `platform_tagging_request_pk` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=3451 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.platform_tagging_users
CREATE TABLE IF NOT EXISTS `platform_tagging_users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `context` enum('auditor','games_explorer') NOT NULL,
  `request_id` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=385 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tagger_requests
CREATE TABLE IF NOT EXISTS `tagger_requests` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `app_id_int` int unsigned DEFAULT NULL,
  `prompt_context` tinytext NOT NULL,
  `state` tinytext,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `input_data` tinytext,
  `progress_data` tinytext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=30069 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tagging_state
CREATE TABLE IF NOT EXISTS `tagging_state` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `state` tinytext NOT NULL,
  `tagger_request_id` int unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `tagging_state_app_id_int_uindex` (`app_id_int`)
) ENGINE=InnoDB AUTO_INCREMENT=41063 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tagging_state_history
CREATE TABLE IF NOT EXISTS `tagging_state_history` (
  `hist_id` int NOT NULL AUTO_INCREMENT,
  `app_id_int` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`hist_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27258 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags
CREATE TABLE IF NOT EXISTS `tags` (
  `app_id_int` int NOT NULL,
  `prompt_row_id` int NOT NULL DEFAULT '0',
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL DEFAULT '0',
  `added_from_store` tinyint NOT NULL DEFAULT '0',
  `removed_from_store` tinyint NOT NULL DEFAULT '0',
  `is_tag_rank_override` tinyint NOT NULL DEFAULT '0',
  `tag_rank_override` smallint NOT NULL DEFAULT '0',
  `overriden_by_user` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`app_id_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_history
CREATE TABLE IF NOT EXISTS `tags_history` (
  `hist_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `prompt_row_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL,
  `added_from_store` tinyint NOT NULL DEFAULT '0',
  `removed_from_store` tinyint NOT NULL DEFAULT '0',
  `is_tag_rank_override` tinyint NOT NULL DEFAULT '0',
  `tag_rank_override` smallint NOT NULL DEFAULT '0',
  PRIMARY KEY (`hist_id`,`app_id_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_history_old
CREATE TABLE IF NOT EXISTS `tags_history_old` (
  `hist_id` int NOT NULL,
  `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `prompt_row_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL,
  `added_from_store` tinyint NOT NULL DEFAULT '0',
  `removed_from_store` tinyint NOT NULL DEFAULT '0',
  `is_tag_rank_override` tinyint NOT NULL DEFAULT '0',
  `tag_rank_override` smallint NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_open
CREATE TABLE IF NOT EXISTS `tags_open` (
  `id` int NOT NULL AUTO_INCREMENT,
  `app_id` tinytext,
  `app_id_int` int NOT NULL,
  `prompt_row_id` int NOT NULL DEFAULT '0',
  `node_id` tinytext NOT NULL,
  `val` tinytext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16560 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_override_from_users
CREATE TABLE IF NOT EXISTS `tags_override_from_users` (
  `app_id_int` int NOT NULL,
  `tag_id_int` int NOT NULL,
  `tag_rank` int NOT NULL,
  `request_id` int NOT NULL,
  PRIMARY KEY (`app_id_int`,`tag_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_override_from_users_apps
CREATE TABLE IF NOT EXISTS `tags_override_from_users_apps` (
  `app_id_int` int NOT NULL,
  PRIMARY KEY (`app_id_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for table gb_tagged_data.tags_single
CREATE TABLE IF NOT EXISTS `tags_single` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `app_id_int` int NOT NULL,
  `tag_id` tinytext NOT NULL,
  `response` mediumtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Data exporting was unselected.

-- Dumping structure for view gb_tagged_data.tags_v
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `tags_v` (
	`app_id_int` INT NOT NULL,
	`tag_id_int` INT NOT NULL,
	`prompt_row_id` BIGINT NOT NULL,
	`tag_rank` INT NOT NULL
);

-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `tags_v`;
CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `gb_tagged_data`.`tags_v` AS select `t`.`app_id_int` AS `app_id_int`,`t`.`tag_id_int` AS `tag_id_int`,`t`.`prompt_row_id` AS `prompt_row_id`,if((`t`.`is_tag_rank_override` = 1),`t`.`tag_rank_override`,`t`.`tag_rank`) AS `tag_rank` from `gb_tagged_data`.`tags` `t` where ((`t`.`removed_from_store` = 0) and (`t`.`overriden_by_user` = 0)) union all select `tt`.`app_id_int` AS `app_id_int`,`tt`.`tag_id_int` AS `tag_id_int`,0 AS `prompt_row_id`,`tt`.`tag_rank` AS `tag_rank` from `gb_tagged_data`.`tags_override_from_users` `tt`
;


-- Dumping database structure for gb_users
CREATE DATABASE IF NOT EXISTS `gb_users` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `gb_users`;

-- Dumping structure for table gb_users.users
CREATE TABLE IF NOT EXISTS `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email_id` int DEFAULT NULL,
  `t` timestamp NOT NULL DEFAULT (now()),
  `deleted_t` timestamp NULL DEFAULT NULL,
  `deleted_by` int DEFAULT NULL,
  `created_by` int DEFAULT NULL,
  `first_name` tinytext,
  `last_name` tinytext,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_users.users_api_keys
CREATE TABLE IF NOT EXISTS `users_api_keys` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `api_key` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `source` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `Index 2` (`user_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_users.users_credentials
CREATE TABLE IF NOT EXISTS `users_credentials` (
  `id` int NOT NULL,
  `password` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `secret` varbinary(100) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_users.users_login_tokens
CREATE TABLE IF NOT EXISTS `users_login_tokens` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `token` char(38) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `created_t` timestamp NOT NULL DEFAULT (now()),
  `expire_days` int NOT NULL,
  `ip_address` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `login_tokens_token_uindex` (`token`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_users.users_password_reset_pending
CREATE TABLE IF NOT EXISTS `users_password_reset_pending` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `guid` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT (now()),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

-- Dumping structure for table gb_users.users_roles
CREATE TABLE IF NOT EXISTS `users_roles` (
  `id` int NOT NULL,
  `role` enum('admin','system','client','guest') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  PRIMARY KEY (`id`,`role`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
