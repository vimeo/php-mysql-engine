CREATE TABLE `enemies` (
                           `id` int(10) NOT NULL AUTO_INCREMENT,
                           `character_id` int(10) NOT NULL,
                           `enemy_id` int(10) NOT NULL,
                           PRIMARY KEY (`id`),
                           KEY `character_id` (`character_id`),
                           KEY `enemy_id` (`enemy_id`)
)
    ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;


INSERT INTO `enemies`
(`id`, `character_id`, `enemy_id`)
VALUES
    (1, 1, 5),
    (2, 2, 5),
    (3, 3, 6),
    (4, 1, 11);


