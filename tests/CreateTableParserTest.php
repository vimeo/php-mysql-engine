<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class CreateTableParserTest extends HackTest {

	public async function testParseSchema(): Awaitable<void> {
		$sql = \file_get_contents(__DIR__.'/fixtures/SchemaExample.sql');

		$expected = dict[
			'test' => shape(
				'name' => 'test',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'VARCHAR',
						'length' => '255',
						'character_set' => 'ascii',
						'collation' => 'ascii_bin',
						'null' => false,
					),
					shape(
						'name' => 'value',
						'type' => 'VARCHAR',
						'length' => '255',
						'character_set' => 'ascii',
						'collation' => 'ascii_bin',
						'null' => false,
					),
				],
				'indexes' => vec[
					shape(
						'type' => 'PRIMARY',
						'cols' => vec[
							shape(
								'name' => 'id',
							),
						],
					),
				],
				'props' => dict[],
				'sql' => 'CREATE TABLE `test` (
  `id` varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `value` varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (`id`)
);',
			),
			'test2' => shape(
				'name' => 'test2',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'BIGINT',
						'length' => '20',
						'unsigned' => true,
						'null' => false,
					),
					shape(
						'name' => 'name',
						'type' => 'VARCHAR',
						'length' => '100',
						'null' => false,
					),
				],
				'indexes' => vec[
					shape(
						'type' => 'PRIMARY',
						'cols' => vec[
							shape(
								'name' => 'id',
							),
							shape(
								'name' => 'name',
							),
						],
					),
					shape(
						'type' => 'INDEX',
						'cols' => vec[
							shape(
								'name' => 'name',
							),
						],
						'name' => 'name',
					),
				],
				'props' => dict[],
				'sql' => 'CREATE TABLE `test2` (
  `id` bigint(20) unsigned NOT NULL,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`,`name`),
  KEY `name` (`name`)
);',
			),
			'test3' => shape(
				'name' => 'test3',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'BIGINT',
						'length' => '20',
						'unsigned' => true,
						'null' => false,
					),
					shape(
						'name' => 'ch',
						'type' => 'CHAR',
						'length' => '64',
						'default' => 'NULL',
						'null' => true,
					),
					shape(
						'name' => 'deleted',
						'type' => 'TINYINT',
						'length' => '3',
						'unsigned' => true,
						'null' => false,
						'default' => '0',
					),
					shape(
						'name' => 'name',
						'type' => 'VARCHAR',
						'length' => '100',
						'null' => false,
					),
				],
				'indexes' => vec[
					shape(
						'type' => 'PRIMARY',
						'cols' => vec[
							shape(
								'name' => 'id',
							),
						],
					),
					shape(
						'type' => 'UNIQUE',
						'cols' => vec[
							shape(
								'name' => 'name',
							),
						],
						'name' => 'name',
					),
				],
				'props' => dict[],
				'sql' => 'CREATE TABLE `test3` (
  `id` bigint(20) unsigned NOT NULL,
  `ch` char(64) DEFAULT NULL,
  `deleted` tinyint(3) unsigned NOT NULL DEFAULT \'0\',
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
);',
			),
		];

		$parser = new CreateTableParser();
		$parsed = $parser->parse($sql);
		expect($parsed['test'])->toHaveSameShapeAs($expected['test']);
		expect($parsed['test2'])->toHaveSameShapeAs($expected['test2']);
		expect($parsed['test3'])->toHaveSameShapeAs($expected['test3']);
	}
}
