<?php

namespace MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;
use MysqlEngine\DataIntegrity;
use MysqlEngine\FakePdoInterface;
use MysqlEngine\Php7\FakePdo;
use MysqlEngine\Schema\Column\DateTime;
use const PHP_MAJOR_VERSION;

class DataIntegrityTest extends TestCase
{

    public function testEmptyDateTimeColumnShouldReturnDefaultDate()
    {
        $dateTimeColumn = new DateTime();

        $result = DataIntegrity::coerceValueToColumn($this->getPdo(), $dateTimeColumn, '');

        $this->assertEquals('0000-00-00 00:00:00', $result);
    }

    private static function getPdo(): FakePdoInterface
    {
        if (PHP_MAJOR_VERSION === 8) {
            return new \MysqlEngine\Php8\FakePdo('mysql:foo;dbname=test;');
        }

        return new FakePdo('mysql:foo;dbname=test;');
    }
}
