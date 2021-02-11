<?php

namespace Vimeo\MysqlEngine\Tests;

use PDOException;

class FakePdoTest extends \PHPUnit\Framework\TestCase
{
    public function tearDown(): void
    {
        \Vimeo\MysqlEngine\Server::reset();
    }

    public function testMinimalTransaction(): void
    {
        $pdo = self::getPdo('mysql:foo;dbname=test;');

        self::assertFalse($pdo->inTransaction());
        $pdo->beginTransaction();
        self::assertTrue($pdo->inTransaction());
        $pdo->commit();
        self::assertFalse($pdo->inTransaction());


        $pdo->beginTransaction();
        self::assertTrue($pdo->inTransaction());
        $pdo->rollBack();
        self::assertFalse($pdo->inTransaction());
    }


    private static function getPdo(string $connection_string, bool $strict_mode = false) : \PDO
    {
        $options = $strict_mode ? [\PDO::MYSQL_ATTR_INIT_COMMAND => 'SET sql_mode="STRICT_ALL_TABLES"'] : [];

        if (\PHP_MAJOR_VERSION === 8) {
            return new \Vimeo\MysqlEngine\Php8\FakePdo($connection_string, '', '', $options);
        }

        return new \Vimeo\MysqlEngine\Php7\FakePdo($connection_string, '', '', $options);
    }
}
