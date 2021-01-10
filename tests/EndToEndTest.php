<?php
namespace Vimeo\MysqlEngine\Tests;

class EndToEndTest extends \PHPUnit\Framework\TestCase
{
	public function tearDown() : void
	{
		\Vimeo\MysqlEngine\Server::reset();
	}

	public function testSelectEmptyResults()
    {
        $pdo = self::getConnectionToFullDB();

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id");
        $query->bindValue(':id', 100);
        $query->execute();

        $this->assertSame([], $query->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function testSelectFetchAssoc()
    {
        $pdo = self::getConnectionToFullDB();

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id ORDER BY `id` ASC");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
        	[
        		['id' => '15'],
        		['id' => '16']
        	],
        	$query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testSelectFetchAssocConverted()
    {
        $pdo = self::getConnectionToFullDB(false);

        $query = $pdo->prepare("SELECT id FROM `video_game_characters` WHERE `id` > :id ORDER BY `id` ASC");
        $query->bindValue(':id', 14);
        $query->execute();

        $this->assertSame(
        	[
        		['id' => 15],
        		['id' => 16]
        	],
        	$query->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    private static function getConnectionToFullDB(bool $emulate_prepares = true) : \PDO
    {
    	$pdo = new \Vimeo\MysqlEngine\FakePdo('mysql:foo;dbname=test;');

    	$pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, $emulate_prepares);

        // create table
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/create_table.sql'))->execute();

        // insertData
        $pdo->prepare(file_get_contents(__DIR__ . '/fixtures/bulk_insert.sql'))->execute();

 		return $pdo;
    }
}
