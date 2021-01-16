<?php
namespace Vimeo\MysqlEngine\Tests;

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

    public function testLeftJoin()
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
                ['name' => 'mario', 'counter' => '0'],
                ['name' => 'luigi', 'counter' => '2'],
                ['name' => 'sonic', 'counter' => '0'],
                ['name' => 'earthworm jim', 'counter' => '4'],
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

    private static function getConnectionToFullDB(bool $emulate_prepares = true) : \PDO
    {
        $pdo = new \Vimeo\MysqlEngine\FakePdo('mysql:foo;dbname=test;');

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
