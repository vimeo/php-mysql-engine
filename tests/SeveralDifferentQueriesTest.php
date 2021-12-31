<?php

namespace MysqlEngine\Tests;

use PDO;
use PHPUnit\Framework\TestCase;
use MysqlEngine\FakePdoInterface;
use MysqlEngine\Php7\FakePdo;

/**
 * Class SeveralDifferentQueriesTest
 * @package MysqlEngine\Tests
 */
class SeveralDifferentQueriesTest extends \PHPUnit\Framework\TestCase
{

    /**
     * @return void
     */
    public function testCreateInsertQuery(): void
    {
        $queries = file_get_contents(__DIR__ . '/fixtures/create_insert_query.sql');
        $pdo = self::getPdo();

        $pdo->prepare($queries)->execute();

        $query = $pdo->query('SELECT * FROM `enemies`');
        $result = (array)$query->fetchAll(\PDO::FETCH_OBJ);
        $result = $this->toArray($result);

        $expected = [
            ['id' => 1, 'character_id' => 1, 'enemy_id' => 5],
            ['id' => 2, 'character_id' => 2, 'enemy_id' => 5],
            ['id' => 3, 'character_id' => 3, 'enemy_id' => 6],
            ['id' => 4, 'character_id' => 1, 'enemy_id' => 11],
        ];
        $this->assertSame($expected, $result);
    }

    /**
     * @return FakePdoInterface
     */
    private static function getPdo(): FakePdoInterface
    {
        if (PHP_MAJOR_VERSION === 8) {
            return new \MysqlEngine\Php8\FakePdo('mysql:foo;dbname=test;');
        }

        return new FakePdo('mysql:foo;dbname=test;');
    }

    /**
     * @param $data
     * @return array
     */
    protected function toArray($data): array
    {
        if (is_array($data)) {
            return array_map(static function ($value) {
                return (array)$value;
            }, $data);
        }
        return (array)$data;
    }

}
