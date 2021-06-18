<?php

declare(strict_types=1);

namespace Vimeo\MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;

class ParameterBindTest extends TestCase
{

    public function dataSqlWithParameter(): array
    {
        return [
            'numeric' => ['SELECT ?', 1],
            ':field'  => ['SELECT :field', ':field'],
            'field'   => ['SELECT :field', 'field'],
        ];
    }

    /**
     * @param string     $sql
     * @param string|int $key
     * @dataProvider dataSqlWithParameter
     */
    public function testBindValue(string $sql, $key): void
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;');

        $statement = $pdo->prepare($sql);
        $statement->bindValue($key, 999);
        $statement->execute();

        self::assertEquals(999, $statement->fetchColumn(0));
    }

    /**
     * @param string     $sql
     * @param string|int $key
     * @dataProvider dataSqlWithParameter
     */
    public function testBindParam(string $sql, $key): void
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;');

        $var       = 1000;
        $statement = $pdo->prepare($sql);
        $statement->bindParam($key, $var);

        $var = 10;
        $statement->execute();
        self::assertEquals(10, $statement->fetchColumn(0));
    }

    private static function getPdo(string $connection_string): \PDO
    {
        if (\PHP_MAJOR_VERSION === 8) {
            return new \Vimeo\MysqlEngine\Php8\FakePdo($connection_string, '', '', []);
        }

        return new \Vimeo\MysqlEngine\Php7\FakePdo($connection_string, '', '', []);
    }
}
