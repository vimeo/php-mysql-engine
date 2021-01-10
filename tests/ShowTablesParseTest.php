<?php
namespace Vimeo\MysqlEngine\Tests;

class ShowTablesParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'SHOW TABLES LIKE \'foo\'';

        $show_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\ShowTablesQuery::class, $show_query);
        $this->assertSame('foo', $show_query->pattern);
    }
}
