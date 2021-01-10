<?php
namespace Vimeo\MysqlEngine\Tests;

class DropParseTest extends \PHPUnit\Framework\TestCase
{
    public function testSimpleParse()
    {
        $query = 'DROP TABLE IF EXISTS `video_game_characters`';

        $drop_query = \Vimeo\MysqlEngine\Parser\SQLParser::parse($query);

        $this->assertInstanceOf(\Vimeo\MysqlEngine\Query\DropTableQuery::class, $drop_query);
        $this->assertSame('video_game_characters', $drop_query->table);
    }
}
