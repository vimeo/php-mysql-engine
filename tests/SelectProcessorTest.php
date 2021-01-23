<?php
namespace Vimeo\MysqlEngine\Tests;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;

class SelectProcessorTest extends \PHPUnit\Framework\TestCase
{
    public function testCast()
    {
        $query = 'SELECT CAST(1 + 2 AS UNSIGNED) as `a`';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = new \Vimeo\MysqlEngine\FakePdo('mysql:foo');

        $this->assertSame(
            [['a' => 3]],
            \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \Vimeo\MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }

    public function testSubqueryCalculation()
    {
        $query = 'SELECT (SELECT 2) + (SELECT 3) as `a`';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = new \Vimeo\MysqlEngine\FakePdo('mysql:foo');

        $this->assertSame(
            [['a' => 5]],
            \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \Vimeo\MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }

    public function testStringDecimalIntComparison()
    {
        $query = 'SELECT ("0.00" > 0) as `a`';

        $select_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);

        $conn = new \Vimeo\MysqlEngine\FakePdo('mysql:foo');

        $this->assertSame(
            [['a' => 0]],
            \Vimeo\MysqlEngine\Processor\SelectProcessor::process(
                $conn,
                new \Vimeo\MysqlEngine\Processor\Scope([]),
                $select_query,
                null
            )->rows
        );
    }
}
