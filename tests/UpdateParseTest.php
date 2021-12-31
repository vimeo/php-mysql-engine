<?php
namespace MysqlEngine\Tests;

class UpdateParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'UPDATE `foo` SET `bar` = `bat` WHERE id = 1';

        $update_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\MysqlEngine\Query\UpdateQuery::class, $update_query);
    }
}
