<?php
namespace Vimeo\MysqlEngine\Tests;

class SelectParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'SELECT `foo` FROM `bar` WHERE `id` = 1';

        $select_query = \Vimeo\MysqlEngine\Parser\SqlParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\SelectQuery::class, $select_query);
    }

    public function testCast()
    {
        $query = 'SELECT CAST(1 + 2 AS UNSIGNED) as `a`';

        $select_query = \Vimeo\MysqlEngine\Parser\SqlParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\SelectQuery::class, $select_query);

        $conn = new \Vimeo\MysqlEngine\FakePdo('mysql:foo');

        $this->assertSame(
            [['a' => 3]],
            \Vimeo\MysqlEngine\Processor\SelectProcessor::process($conn, $select_query)
        );
    }

    public function testComplex()
    {
        $query = 'SELECT IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)';

        $select_query = \Vimeo\MysqlEngine\Parser\SqlParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\SelectQuery::class, $select_query);

        $this->assertCount(1, $select_query->selectExpressions);

        $this->assertInstanceOf(
            \Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression::class,
            $select_query->selectExpressions[0]
        );

        $this->assertTrue($select_query->selectExpressions[0]->isWellFormed());

        $this->assertSame('IFNULL(`a`.`b`, 0) + ISNULL(`a`.`c`)', $select_query->selectExpressions[0]->name);
    }
}
