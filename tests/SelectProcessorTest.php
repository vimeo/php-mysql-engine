<?php
namespace MysqlEngine\Tests;

use MysqlEngine\Query\SelectQuery;
use MysqlEngine\Query\Expression\ColumnExpression;
use MysqlEngine\Query\Expression\CaseOperatorExpression;
use MysqlEngine\Query\Expression\BinaryOperatorExpression;

class SelectProcessorTest extends \PHPUnit\Framework\TestCase
{
    public function testCast()
    {
        $query = 'SELECT CAST(1 + 2 AS UNSIGNED) as `a`';

        $select_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = self::getPdo('mysql:foo;dbname=test;');

        $this->assertSame(
            [['a' => 3]],
            \MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }

    public function testSubqueryCalculation()
    {
        $query = 'SELECT (SELECT 2) + (SELECT 3) as `a`';

        $select_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = self::getPdo('mysql:foo;dbname=test;');

        $this->assertSame(
            [['a' => 5]],
            \MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }

    public function testStringDecimalIntComparison()
    {
        $query = 'SELECT ("0.00" > 0) as `a`';

        $select_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = self::getPdo('mysql:foo;dbname=test;');

        $this->assertSame(
            [['a' => 0]],
            \MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }

    private static function getPdo(string $connection_string) : \PDO
    {
        if (\PHP_MAJOR_VERSION === 8) {
            return new \MysqlEngine\Php8\FakePdo($connection_string);
        }

        return new \MysqlEngine\Php7\FakePdo($connection_string);
    }
}
