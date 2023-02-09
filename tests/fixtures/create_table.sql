CREATE TABLE `video_game_characters` (
	`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` varchar(32) NOT NULL DEFAULT '',
	`bio_en` text NOT NULL,
	`bio_fr` text,
	`type` enum('hero', 'villain') NOT NULL DEFAULT 'hero',
	`profession` varchar(30) CHARACTER SET utf8mb4 DEFAULT NULL,
	`console` enum('atari', 'gameboy', 'nes', 'pc', 'sega genesis', 'super nintendo') DEFAULT NULL,
	`is_alive` tinyint(3) NOT NULL DEFAULT '1',
	`powerups` tinyint(3) UNSIGNED NOT NULL DEFAULT '0',
	`skills` varchar(1000) NOT NULL DEFAULT '',
	`nullable_field` tinyint(3) DEFAULT NULL,
	`nullable_field_default_0` tinyint(3) DEFAULT '0',
	`some_float` float DEFAULT '0.00',
	`total_games` int(11) UNSIGNED NOT NULL DEFAULT '0',
	`lives` int(11) UNSIGNED NOT NULL DEFAULT '0',
	`created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`modified_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`deleted_on` timestamp NULL DEFAULT NULL,
	PRIMARY KEY (`id`),
	KEY `name` (`name`),
	KEY `co_index` (`profession`, `powerups`),
	KEY `app_co_index` (`profession`, `is_alive`, `powerups`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `enemies` (
	`id` int(10) NOT NULL AUTO_INCREMENT,
	`character_id` int(10) NOT NULL,
	`enemy_id` int(10) NOT NULL,
	PRIMARY KEY (`id`),
	KEY `character_id` (`character_id`),
	KEY `enemy_id` (`enemy_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `character_tags` (
	`id` int(10) NOT NULL AUTO_INCREMENT,
	`character_id` int(10) NOT NULL,
	`tag` varchar(30) NOT NULL,
	PRIMARY KEY (`id`),
	KEY `character_id` (`character_id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `transactions` (
	`id` int(10) NOT NULL AUTO_INCREMENT,
	`total` DECIMAL(12, 2) NOT NULL,
	`tax` DECIMAL(12, 2) NOT NULL,
	`other_tax` DECIMAL(12, 2) DEFAULT NULL,
	PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `orders`
(
	`id` INTEGER(11) UNSIGNED NOT NULL AUTO_INCREMENT,
	`user_id` INTEGER(11) UNSIGNED,
	`price` INTEGER(11) UNSIGNED NOT NULL DEFAULT 0,
	`created_on`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`modified_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `tweets` (
	`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`title` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	`text` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `texts` (
	`id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`title_char_col` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	`title_col` varchar(256) COLLATE utf8mb4_unicode_ci,
	`title` varchar(256)
	`text_char_col` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	`text_col` text COLLATE utf8mb4_unicode_ci,
	`text` text,
	PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
