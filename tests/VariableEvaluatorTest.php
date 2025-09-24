<?php

declare(strict_types=1);

namespace Vimeo\MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;

class VariableEvaluatorTest extends TestCase
{
    protected function tearDown(): void
    {
        \Vimeo\MysqlEngine\Server::reset();
    }

    public function testSessionTimeZone(): void
    {
        $query = $this->getPdo()
            ->prepare('SELECT @@session.time_zone');
        $query->execute();
        $result = $query->fetch(\PDO::FETCH_COLUMN);

        $this->assertSame(date_default_timezone_get(), $result);
    }

    public function testNotImplementedGlobalVariable(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        $this->expectExceptionMessage("The SQL code SELECT @@collation_server; could not be evaluated");

        $query = $this->getPdo()
            ->prepare('SELECT @@collation_server;');
        $query->execute();
        $result = $query->fetch(\PDO::FETCH_COLUMN);

        $this->assertSame(date_default_timezone_get(), $result);
    }

    public function testVariable(): void
    {
        $sql = "
            SELECT (@var := @var + 2) AS `counter`
            FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `virtual_rows`
            CROSS JOIN (SELECT @var := 0) AS `vars`;
        ";

        $query = $this->getPdo()
            ->prepare($sql);
        $query->execute();
        $result = $query->fetchAll(\PDO::FETCH_ASSOC);
        $counters = array_map('intval', array_column($result, 'counter'));
        $this->assertSame([2,4,6], $counters);
    }

    private function getPdo(bool $strict_mode = false): \PDO
    {
        $connection_string = 'mysql:foo;dbname=test;';
        $options = $strict_mode ? [\PDO::MYSQL_ATTR_INIT_COMMAND => 'SET sql_mode="STRICT_ALL_TABLES"'] : [];

        if (\PHP_MAJOR_VERSION === 8) {
            return new \Vimeo\MysqlEngine\Php8\FakePdo($connection_string, '', '', $options);
        }

        return new \Vimeo\MysqlEngine\Php7\FakePdo($connection_string, '', '', $options);
    }
}
