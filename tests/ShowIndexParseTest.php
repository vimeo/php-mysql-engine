<?php

namespace MysqlEngine\Tests;

use PHPUnit\Framework\TestCase;
use MysqlEngine\Parser\SQLParser;
use MysqlEngine\Query\SelectQuery;
use MysqlEngine\Query\ShowIndexQuery;

class ShowIndexParseTest extends TestCase
{
    public function testSimpleParse()
    {
        $query = 'SHOW INDEX FROM `foo`';

        $show_query = SQLParser::parse($query);

        $this->assertInstanceOf(ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }

    public function testIndexesParse()
    {
        $query = 'SHOW INDEXES FROM `foo`';

        $show_query = SQLParser::parse($query);

        $this->assertInstanceOf(ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }

    public function testKeysParse()
    {
        $query = 'SHOW KEYS FROM `foo`';

        $show_query = SQLParser::parse($query);

        $this->assertInstanceOf(ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }

    public function testParseInvalid()
    {
        $query = 'SHOW INDEX FROM `foo';

        $this->expectException(\MysqlEngine\Parser\LexerException::class);

        $select_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(SelectQuery::class, $select_query);
    }

    public function testWhereParse()
    {
        $query = "SHOW INDEX FROM `foo` WHERE `Key_name` = 'PRIMARY'";

        $show_query = SQLParser::parse($query);

        $this->assertInstanceOf(ShowIndexQuery::class, $show_query);
        $this->assertSame('foo', $show_query->table);
    }
}
