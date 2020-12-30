<?php
namespace Vimeo\MysqlEngine\Tests;

class UpdateParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'UPDATE `foo` SET `bar` = `bat` WHERE id = 1';

        $update_query = \Vimeo\MysqlEngine\Parser\SqlParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\UpdateQuery::class, $update_query);
    }
}
