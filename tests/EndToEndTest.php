<?php
namespace Vimeo\MysqlEngine\Tests;

use PDOException;

class EndToEndTest extends \PHPUnit\Framework\TestCase
{
    public function tearDown() : void
    {
        \Vimeo\MysqlEngine\Server::reset();
    }

    public function testSelectEmptyResults()
    {
        $pdo = self::getConnectionToFullDB();

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id");
        $query->bindValue(':id', 100);
        $query->execute();

        $this->assertSame([], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testInvalidQuery()
    {
        $pdo = self::getConnectionToFullDB();

        $this->expectException(\UnexpectedValueException::class);

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id > :id");
        $query->bindValue(':id', 100);
        $query->execute();

        $this->assertSame([], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testSelectFetchAssoc()
    {
        $pdo = self::getConnectionToFullDB();

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id ORDER BY `id` ASC");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
            [
                ['id' => '15'],
                ['id' => '16']
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSelectFetchAssocConverted()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id ORDER BY `id` ASC");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
            [
                ['id' => 15],
                ['id' => 16]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testPlaceholders()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?");
        $query->bindValue(1, 14);
        $query->bindValue(2, 1);
        $query->execute();

        $this->assertSame(
            [
                ['id' => 15]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > ? ORDER BY `id` ASC LIMIT ?");
        $query->execute([14, 1]);

        $this->assertSame(
            [
                ['id' => 15]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSumEmptySet()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT SUM(`id`) FROM `video_game_characters` WHERE `id` > :id");
        $query->bindValue(':id', 100);
        $query->execute();

        $this->assertSame([['SUM(`id`)' => null]], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testDefaultNullTimestamp()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT `deleted_on` FROM `video_game_characters` WHERE `id` = 1");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
            [
                ['deleted_on' => null],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSelectCountFullResults()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT COUNT(*) FROM `video_game_characters`");
        $query->execute();

        $this->assertSame([['COUNT(*)' => 16]], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testSelectCountEmptyResults()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT COUNT(*) FROM `video_game_characters` WHERE `id` > :id");
        $query->bindValue(':id', 100);
        $query->execute();

        $this->assertSame([['COUNT(*)' => 0]], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testAliasWithType()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT SUM(`a`) FROM (SELECT `id` as `a` FROM `video_game_characters`) `foo`");

        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
            [
                ['SUM(`a`)' => 136]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testAliasName()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT `a` FROM (SELECT SUM(`id`) as `a` FROM `video_game_characters`) `foo`");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
            [
                ['a' => 136]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testLeftJoinWithSum()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            "SELECT SUM(`powerups`) as `p`
            FROM `video_game_characters`
            LEFT JOIN `character_tags` ON `character_tags`.`character_id` = `video_game_characters`.`id`"
        );
        $query->execute();

        $this->assertSame(
            [
                ['p' => 21]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testLeftJoinWithCount()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `name`,
                    `tag_totals`.`c`
                FROM `video_game_characters`
                LEFT JOIN (
                    SELECT COUNT(*) as `c`, `character_tags`.`character_id`
                    FROM `character_tags`
                    GROUP BY `character_tags`.`character_id`) AS `tag_totals`
                ON `tag_totals`.`character_id` = `video_game_characters`.`id`
                ORDER BY `id`
                LIMIT 5'
        );
        $query->execute();

        $this->assertSame(
            [
                ['name' => 'mario', 'c' => 2],
                ['name' => 'luigi', 'c' => 3],
                ['name' => 'sonic', 'c' => null],
                ['name' => 'earthworm jim', 'c' => null],
                ['name' => 'bowser', 'c' => 2]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testMaxValueAliasedToColumnName()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `character_id`, MAX(`id`) as `id`
                FROM `character_tags`
                GROUP BY `character_id`
                LIMIT 3'
        );
        $query->execute();

        $this->assertSame(
            [
                ['character_id' => 1, 'id' => 2],
                ['character_id' => 2, 'id' => 5],
                ['character_id' => 5, 'id' => 7],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testIncrementCounter()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `name`, (@var := @var + 2) AS `counter`
                FROM `video_game_characters`
                CROSS JOIN (SELECT @var := 0) as `vars`
                WHERE `type` = \'hero\'
                LIMIT 3'
        );

        $query->execute();

        $this->assertSame(
            [
                ['name' => 'mario', 'counter' => 2],
                ['name' => 'luigi', 'counter' => 4],
                ['name' => 'sonic', 'counter' => 6],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testAssignUndefinedIntToVariable()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT @a := `id` as `id`, @b := @a AS `id_copy`
                FROM `video_game_characters`
                LIMIT 3'
        );

        $query->execute();

        $this->assertSame(
            [
                ['id' => 1, 'id_copy' => '1'],
                ['id' => 2, 'id_copy' => '2'],
                ['id' => 3, 'id_copy' => '3'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testConditionallyIncrementedCounter()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `name`, CASE WHEN `id` % 2 = 0 THEN (@var := @var + 2) ELSE 0 END AS `counter`
                FROM `video_game_characters`
                CROSS JOIN (SELECT @var := 0) as `vars`
                WHERE `type` = \'hero\'
                LIMIT 4'
        );

        $query->execute();

        $this->assertSame(
            [
                ['name' => 'mario', 'counter' => 0],
                ['name' => 'luigi', 'counter' => 2],
                ['name' => 'sonic', 'counter' => 0],
                ['name' => 'earthworm jim', 'counter' => 4],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testPreviousCurrentTempVariables()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT
                    @previous AS `previous`,
                    @previous := `e`.`name` AS `current`
                FROM (SELECT @previous := NULL) AS `init`,
                    `video_game_characters` AS `e`
                ORDER BY `e`.`id`
                LIMIT 3'
        );

        $query->execute();

        $this->assertSame(
            [
                ['previous' => null, 'current' => 'mario'],
                ['previous' => 'mario', 'current' => 'luigi'],
                ['previous' => 'luigi', 'current' => 'sonic'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testPreviousCurrentNextBackwardsTempVariables()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT
                    @next AS `next`,
                    @next := `current` AS `current`,
                    `previous`
                FROM
                (
                    SELECT @next := NULL
                ) AS `init`,
                (
                    SELECT
                        @previous AS `previous`,
                        @previous := `e`.`name` AS `current`,
                        `e`.`id`
                    FROM (SELECT @previous := NULL) AS `init`,
                        `video_game_characters` AS `e`
                    ORDER BY `e`.`id`
                ) AS `a`
                ORDER BY `a`.`id` DESC
                LIMIT 3'
        );

        $query->execute();

        $this->assertSame(
            [
                ['next' => null, 'current' => 'dude', 'previous' => 'link'],
                ['next' => 'dude', 'current' => 'link', 'previous' => 'yoshi'],
                ['next' => 'link', 'current' => 'yoshi', 'previous' => 'pac man'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testPreviousCurrentNextTempVariables()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `previous`, `current`, `next`
                FROM
                (
                    SELECT
                        @next AS `next`,
                        @next := `current` AS `current`,
                        `previous`,
                        `id`
                    FROM
                    (
                        SELECT @next := NULL
                    ) AS `init`,
                    (
                    SELECT
                        @previous AS `previous`,
                        @previous := `e`.`name` AS `current`,
                        `e`.`id`
                    FROM (SELECT @previous := NULL) AS `init`,
                        `video_game_characters` AS `e`
                    ORDER BY `e`.`id`
                    ) AS `a`
                    ORDER BY `a`.`id` DESC
                ) AS `b`
                ORDER BY `id`
                LIMIT 3'
        );

        $query->execute();

        $this->assertSame(
            [
                ['previous' => null, 'current' => 'mario', 'next' => 'luigi'],
                ['previous' => 'mario', 'current' => 'luigi', 'next' => 'sonic'],
                ['previous' => 'luigi', 'current' => 'sonic', 'next' => 'earthworm jim'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testDateArithhmetic()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare(
            'SELECT DATE_SUB(\'2020-03-01 12:00:00\', INTERVAL 1 HOUR) as `a`,
                    DATE_ADD(\'2020-03-01 12:00:00\', INTERVAL 1 HOUR) as `b`,
                    DATEDIFF(\'2017-01-01\', \'2016-12-24\') AS `c`,
                    DATE(\'2020-03-01 12:00:00\') as `d`,
                    LAST_DAY(\'2020-03-01 12:00:00\') as `e`,
                    DATE_ADD(\'2018-01-31 12:31:00\', INTERVAL 1 MONTH) as `f`,
                    DATE_ADD(\'2020-02-29 12:31:00\', INTERVAL 1 YEAR) as `g`,
                    DATE_ADD(\'2020-02-29 12:31:00\', INTERVAL 4 YEAR) as `h`,
                    DATE_SUB(\'2020-03-30\', INTERVAL 1 MONTH) As `i`,
                    DATE_SUB(\'2020-03-01\', INTERVAL 1 MONTH) As `j`,
                    WEEKDAY(\'2021-04-29\') AS `k`'
        );

        $query->execute();

        $this->assertSame(
            [[
                'a' => '2020-03-01 11:00:00',
                'b' => '2020-03-01 13:00:00',
                'c' => 8,
                'd' => '2020-03-01',
                'e' => '2020-03-31',
                'f' => '2018-02-28 12:31:00',
                'g' => '2021-02-28 12:31:00',
                'h' => '2024-02-29 12:31:00',
                'i' => '2020-02-29',
                'j' => '2020-02-01',
                'k' => 4,
            ]],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testCurDateFunction()
    {
        $pdo = self::getPdo('mysql:foo');

        $query = $pdo->prepare('SELECT CURDATE() AS date, CURRENT_DATE() AS date1');

        $query->execute();
        $current_date = date('Y-m-d');

        $this->assertSame(
            [[
                'date' => $current_date,
                'date1' => $current_date,
            ]],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testInOperator()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare(
            'SELECT (2 in (2, 0)) as `t`'
        );

        $query->execute();

        $this->assertSame(
            [[
                't' => 1,
            ]],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testEmptyStringEqualsZero()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare(
            'SELECT (0 = \'\') as `t`'
        );

        $query->execute();

        $this->assertSame(
            [[
                't' => 1,
            ]],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testDecimalArithhmetic()
    {
        $pdo = self::getConnectionToFullDB(false);

        $pdo->prepare(
            'INSERT INTO `transactions` (`total`, `tax`) VALUES (1.00, 0.10), (2.00, 0.20)'
        )->execute();

        $query = $pdo->prepare('SELECT `total` - `tax` AS `diff` FROM `transactions`');

        $query->execute();

        $this->assertSame(
            [
                ['diff' => '0.90'],
                ['diff' => '1.80']
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testInetAtoN()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare("SELECT INET_ATON('255.255.255.255') AS a, INET_ATON('192.168.1.1') AS b, INET_ATON('127.0.0.1') AS c, INET_ATON('not an ip') AS d, INET_ATON(NULL) as e");
        $query->execute();
        $this->assertSame(
            [
                [
                    'a' => 4294967295,
                    'b' => 3232235777,
                    'c' => 2130706433,
                    'd' => NULL,
                    'e' => NULL,
                ],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }


    public function testInetNtoA()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare("SELECT INET_NTOA(4294967295) AS a, INET_NTOA(3232235777) AS b, INET_NTOA(2130706433) AS c, INET_NTOA(NULL) as d, INET_NTOA('not a number') as e");
        $query->execute();

        $this->assertSame(
            [
                [
                    'a' => '255.255.255.255',
                    'b' => '192.168.1.1',
                    'c' => '127.0.0.1',
                    'd' => NULL,
                    'e' => NULL,
                ],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }


    public function testRound()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare('SELECT ROUND(3.141592) AS a, ROUND(3.141592, 2) AS b');

        $query->execute();

        $this->assertSame(
            [
                [
                    'a' => 3,
                    'b' => 3.14,
                ],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testCeil()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare('SELECT CEIL(3.1) AS a, CEILING(4.7) AS b, CEIL("abc") AS c, CEIL(5) AS d, CEIL("5.5") AS e');

        $query->execute();

        $this->assertSame(
            [
                [
                    'a' => 4,
                    'b' => 5,
                    'c' => 0.0,
                    'd' => 5,
                    'e' => 6.0,
                ],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testFloor()
    {
        $pdo = self::getPdo('mysql:foo');
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

        $query = $pdo->prepare('SELECT FLOOR(3.1) AS a, FLOOR(4.7) AS b, FLOOR("abc") AS c, FLOOR(5) AS d, FLOOR("6.5") AS e');

        $query->execute();

        $this->assertSame(
            [
                [
                    'a' => 3,
                    'b' => 4,
                    'c' => 0.0,
                    'd' => 5,
                    'e' => 6.0,
                ],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testIsInFullSubquery()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT 16 IN (SELECT `id` from `video_game_characters`) as `is_in`');

        $query->execute();

        $this->assertSame(
            [
                ['is_in' => 1]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testIsNotInEmptySet()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT 16 NOT IN (SELECT `id` from `video_game_characters` WHERE `id` > 16) as `isnt`');

        $query->execute();

        $this->assertSame(
            [
                ['isnt' => 1]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testExistsEmptySubquery()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT EXISTS(SELECT `id` from `video_game_characters` WHERE `id` > 16) as `does_exist`');

        $query->execute();

        $this->assertSame(
            [
                ['does_exist' => 0]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testExistsNotEmptySubquery()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT EXISTS(SELECT `id` from `video_game_characters` WHERE `id` > 5) as `does_exist`');

        $query->execute();

        $this->assertSame(
            [
                ['does_exist' => 1]
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSelectHavingCount()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `console`
            FROM `video_game_characters`
            GROUP BY `console`
            HAVING COUNT(*) > 2'
        );

        $query->execute();

        $this->assertSame(
            [
                ['console' => 'nes'],
                ['console' => 'sega genesis'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSelectHavingOnAliasField()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `console`, COUNT(*) as `c`
            FROM `video_game_characters`
            GROUP BY `console`
            HAVING `c` > 2'
        );

        $query->execute();

        $this->assertSame(
            [
                ['console' => 'nes', 'c' => 9],
                ['console' => 'sega genesis', 'c' => 4],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testLastInsertIdAfterSkippingAutoincrement()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                (`id`, `name`, `type`, `profession`, `console`, `is_alive`, `powerups`, `skills`, `created_on`)
            VALUES
                (20, 'wario','villain','plumber','nes','1','3','{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}', NOW())"
        );

        $query->execute();

        $this->assertSame("20", $pdo->lastInsertId());

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                SET `name` = 'wario2',
                    `type` = 'villain',
                    `profession` = 'plumber',
                    `console` = 'nes',
                    `is_alive` = '1',
                    `powerups` = '3',
                    `skills` = '{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}',
                    `created_on` = NOW()"
        );

        $query->execute();

        $this->assertSame("21", $pdo->lastInsertId());

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                (`name`, `type`, `profession`, `console`, `is_alive`, `powerups`, `skills`, `created_on`)
            VALUES
                ('wario3','villain','plumber','nes','1','3','{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}', NOW())"
        );

        $query->execute();

        $this->assertSame("22", $pdo->lastInsertId());
    }

    public function testInsertWithNullAndEmpty()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                (`id`, `name`, `type`, `profession`, `console`, `is_alive`, `powerups`, `skills`, `created_on`)
            VALUES
                (20, 'wario','villain','plumber','nes','1','3','{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}', NOW())"
        );

        $query->execute();

        $query = $pdo->prepare(
            'SELECT `bio_en`, `bio_fr`
            FROM `video_game_characters`
            ORDER BY `id` DESC
            LIMIT 1'
        );

        $query->execute();

        $this->assertSame(
            [
                ['bio_en' => '', 'bio_fr' => null],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testOrderBySecondDimensionAliased()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `id`, `console` AS `console_name`
            FROM `video_game_characters`
            ORDER BY `console`, `powerups`, `name`
            LIMIT 4'
        );

        $query->execute();

        $this->assertSame(
            [
                ['id' => 13, 'console_name' => 'atari'],
                ['id' => 9, 'console_name' => 'gameboy'],
                ['id' => 5, 'console_name' => 'nes'],
                ['id' => 11, 'console_name' => 'nes'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testOrderByAliasedSecondDimension()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT `id`, `console` AS `console_name`
            FROM `video_game_characters`
            ORDER BY `console_name`, `powerups`, `name`
            LIMIT 4'
        );

        $query->execute();

        $this->assertSame(
            [
                ['id' => 13, 'console_name' => 'atari'],
                ['id' => 9, 'console_name' => 'gameboy'],
                ['id' => 5, 'console_name' => 'nes'],
                ['id' => 11, 'console_name' => 'nes'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testGroupByAliasHavingNoAlias()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT count(*) AS `c`, `console` as `n` FROM `video_game_characters` GROUP BY `n` HAVING count(*) > 1'
        );

        $query->execute();

        $this->assertSame(
            [
                ['c' => 9, 'n' => 'nes'],
                ['c' => 4, 'n' => 'sega genesis'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testGroupByAliasHavingAlias()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT count(*) AS `c`, `console` as `n` FROM `video_game_characters` GROUP BY `n` HAVING `c` > 1'
        );

        $query->execute();

        $this->assertSame(
            [
                ['c' => 9, 'n' => 'nes'],
                ['c' => 4, 'n' => 'sega genesis'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testHavingAliasSelectColumn()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT LENGTH(`console`) as `l`, `name`, `type`
            FROM `video_game_characters`
            HAVING `l` > 4 AND `type` = "hero"
            ORDER BY `l` LIMIT 2'
        );

        $query->execute();

        $this->assertSame(
            [
                ['l' => 5, 'name' => 'pac man', 'type' => 'hero'],
                ['l' => 7, 'name' => 'pikachu', 'type' => 'hero'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testDistinctColumn()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare(
            'SELECT DISTINCT `console` AS `console`
            FROM `video_game_characters`
            ORDER BY `console`'
        );

        $query->execute();

        $this->assertEquals(
            [
                ['console' => 'atari'],
                ['console' => 'gameboy'],
                ['console' => 'nes'],
                ['console' => 'sega genesis'],
                ['console' => 'super nintendo'],
            ],
            $query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testInsertOutOfRangeStrict()
    {
        $pdo = self::getConnectionToFullDB(false, true);

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                (`name`, `type`, `profession`, `console`, `is_alive`, `powerups`, `skills`, `created_on`)
            VALUES
                ('wario','villain','plumber','nes','1','-4','{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}', NOW())"
        );

        $this->expectException(\Vimeo\MysqlEngine\Processor\InvalidValueException::class);

        $query->execute();
    }

    public function testInsertOutOfRangeLenient()
    {
        $pdo = self::getConnectionToFullDB(false, false);

        $query = $pdo->prepare(
            "INSERT INTO `video_game_characters`
                (`name`, `type`, `profession`, `console`, `is_alive`, `powerups`, `skills`, `created_on`)
            VALUES
                ('wario','villain','plumber','nes','1','-4','{\"magic\":0, \"speed\":0, \"strength\":0, \"weapons\":0}', NOW())"
        );

        $query->execute();
    }

    public function testFetchCount()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT count(*) FROM `video_game_characters`');

        $query->execute();

        $this->assertGreaterThan(0, $query->fetchColumn());

        $this->assertFalse($query->fetchColumn());
    }

    public function dataProviderFetchCountForMissingColumn(): \Generator
    {
        foreach ([-1, 1, 100] as $idx) {
            yield 'column: '.$idx => [$idx];
        }
    }

    /**
     * @dataProvider dataProviderFetchCountForMissingColumn
     */
    public function testFetchCountForMissingColumn(int $columnIndex)
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT `id` FROM `video_game_characters` LIMIT 1');

        $query->execute();

        $this->expectException(PDOException::class);

        $query->fetchColumn($columnIndex);
    }

    public function testFetchWithColumnMode()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare('SELECT `id` FROM `video_game_characters` WHERE id=2 LIMIT 1');

        $query->execute();

        self::assertEquals(2, $query->fetch(\PDO::FETCH_COLUMN));
    }

    public function dataProviderTruncateForms(): \Generator
    {
        foreach ([
                'short'       => 'TRUNCATE',
                'long'        => 'TRUNCATE TABLE',
                'lower short' => 'truncate',
                'lower long'  => 'truncate table',
            ] as $formName => $formSql
        ) {
            foreach ([
                    'relative table' => '`video_game_characters`',
                    'absolute table' => '`test`.`video_game_characters`',
                ] as $position => $table
            ) {
                yield $formName . ' + ' . $position => [$formSql, $table];
            }
        };
    }

    /**
     * @param string $truncateForm
     * @param string $table
     *
     * @dataProvider dataProviderTruncateForms
     */
    public function testTruncate(string $truncateForm, string $table)
    {
        $pdo = self::getConnectionToFullDB(false);

        // check that table some data
        $this->assertGreaterThan(
            0,
            $pdo->query('SELECT count(*) FROM '.$table)->fetchColumn(0)
        );
        $pdo->exec($truncateForm . ' '.$table);
        $this->assertEquals(
            0,
            $pdo->query('SELECT count(*) FROM '.$table)->fetchColumn(0)
        );
    }

    public function dataProviderDropTable(): array
    {
        return [
            'relative'  => ['`video_game_characters`'],
            'absolute' => ['`test`.`video_game_characters`'],
        ];
    }
    /**
     * @param string $table
     *
     * @dataProvider dataProviderDropTable
     */
    public function testDropTable(string $table): void
    {
        $pdo = self::getConnectionToFullDB(false);

        // checking that table exists
        $this->assertNotFalse(
            $pdo->query('SHOW TABLES LIKE "video_game_characters"')->fetchColumn(0)
        );

        // remove table
        $pdo->exec('DROP TABLE '.$table);

        // checking that table is missing
        $this->assertFalse(
            $pdo->query('SHOW TABLES LIKE "video_game_characters"')->fetchColumn(0)
        );
    }

    public function testSelectNullableFields()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT nullable_field, nullable_field_default_0 FROM `video_game_characters` WHERE `id` = 1");
        $query->execute();

        $this->assertSame(
            ['nullable_field' => null, 'nullable_field_default_0' => 0],
            $query->fetch(\PDO::FETCH_ASSOC)
        );

        $query = $pdo->prepare("UPDATE `video_game_characters` SET `nullable_field_default_0` = NULL, `nullable_field` = NULL WHERE `id` = 1");
        $query->execute();

        $query = $pdo->prepare("SELECT nullable_field, nullable_field_default_0 FROM `video_game_characters` WHERE `id` = 1");
        $query->execute();

        $this->assertSame(
            ['nullable_field' => null, 'nullable_field_default_0' => null],
            $query->fetch(\PDO::FETCH_ASSOC)
        );
    }

    public function testUpdateWithOrderAndPrimaryKey()
    {
        $pdo = self::getConnectionToFullDB(false);
        $pdo->exec('UPDATE `video_game_characters` SET `bio_en` = "spiky boi" WHERE `id` = 7 ORDER BY `id` DESC');
    }


    private static function getPdo(string $connection_string, bool $strict_mode = false) : \PDO
    {
        $options = $strict_mode ? [\PDO::MYSQL_ATTR_INIT_COMMAND => 'SET sql_mode="STRICT_ALL_TABLES"'] : [];

        if (\PHP_MAJOR_VERSION === 8) {
            return new \Vimeo\MysqlEngine\Php8\FakePdo($connection_string, '', '', $options);
        }

        return new \Vimeo\MysqlEngine\Php7\FakePdo($connection_string, '', '', $options);
    }

    private static function getConnectionToFullDB(bool $emulate_prepares = true, bool $strict_mode = false) : \PDO
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;', $strict_mode);

        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, $emulate_prepares);

        // create table
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/create_table.sql'))->execute();

        // insertData
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/bulk_character_insert.sql'))->execute();
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/bulk_enemy_insert.sql'))->execute();
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/bulk_tag_insert.sql'))->execute();

        return $pdo;
    }
}
