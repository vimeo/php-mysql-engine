CREATE TABLE `test` (
  `id` varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `value` varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `test2` (
  `id` bigint(20) unsigned NOT NULL,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`,`name`),
  KEY `name` (`name`)
);

CREATE TABLE `test3` (
  `id` bigint(20) unsigned NOT NULL,
  `ch` char(64) DEFAULT NULL,
  `deleted` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
);