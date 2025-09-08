<?php

declare(strict_types=1);

namespace Vimeo\MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;

class FunctionEvaluatorTest extends TestCase
{

    public function tearDown() : void
    {
        \Vimeo\MysqlEngine\Server::reset();
    }

    /**
     * @dataProvider maxValueProvider
     */
    public function testSqlMax(string $sql, ?string $expected, bool $is_db_number) : void
    {
        $query = self::getConnectionToFullDB()->prepare($sql);
        $query->execute();
        /** @var array<array<string, string|null>> $result */
        $result = $query->fetchAll(\PDO::FETCH_ASSOC);

        if ($is_db_number) {
            $this->assertNotEmpty($result);
            $this->assertNotNull($result[0]['max']);
        } else {
            $this->assertSame([['max' => $expected]], $result);
        }
    }

    public static function maxValueProvider(): array
    {
        return [
            'null when no rows' => [
                'sql' => 'SELECT MAX(null) as `max` FROM `video_game_characters`',
                'expected' => null,
                'is_db_number' => false,
            ],
            'max of scalar values' => [
                'sql' => 'SELECT MAX(10) as `max` FROM `video_game_characters`',
                'expected' => '10',
                'is_db_number' => false,
            ],
            'max in DB values' => [
                'sql' => 'SELECT MAX(id) as `max` FROM `video_game_characters`',
                'expected' => '',
                'is_db_number' => true,
            ],
        ];
    }

    /**
     * @dataProvider minValueProvider
     */
    public function testSqlMin(string $sql, ?string $expected, bool $is_db_number) : void
    {
        $query = self::getConnectionToFullDB()->prepare($sql);
        $query->execute();
        /** @var array<array<string, string|null>> $result */
        $result = $query->fetchAll(\PDO::FETCH_ASSOC);

        if ($is_db_number) {
            $this->assertNotEmpty($result);
            $this->assertNotNull($result[0]['min']);
        } else {
            $this->assertSame([['min' => $expected]], $result);
        }
    }

    public static function minValueProvider(): array
    {
        return [
            'null when no rows' => [
                'sql' => 'SELECT MIN(null) as `min` FROM `video_game_characters`',
                'expected' => null,
                'is_db_number' => false,
            ],
            'min of scalar values' => [
                'sql' => 'SELECT MIN(10) as `min` FROM `video_game_characters`',
                'expected' => '10',
                'is_db_number' => false,
            ],
            'min in DB values' => [
                'sql' => 'SELECT MIN(id) as `min` FROM `video_game_characters`',
                'expected' => '',
                'is_db_number' => true,
            ],
        ];
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