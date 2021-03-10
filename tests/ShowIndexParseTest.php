<?php
namespace Vimeo\MysqlEngine\Tests;

class ShowIndexParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'SHOW INDEX FROM foo';

        $show_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }

    public function testIndexesParse()
    {
        $query = 'SHOW INDEXES FROM foo';

        $show_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }

    public function testKeysParse()
    {
        $query = 'SHOW KEYS FROM foo';

        $show_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }
}
