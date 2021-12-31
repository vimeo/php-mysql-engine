<?php
namespace MysqlEngine\Tests;

class DropParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'DROP TABLE IF EXISTS `video_game_characters`';

        $drop_query = \MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\MysqlEngine\Query\DropTableQuery::class, $drop_query);
        $this->assertSame('video_game_characters', $drop_query->table);
    }
}
