<?hh // strict

namespace Slack\DBMock;

final class SharedSetup {
	public static function init(bool $strict): void {
		init(TEST_SCHEMA, $strict);
	}
}

const dict<string, dict<string, table_schema>> TEST_SCHEMA = dict[
	'db1' => dict[
		'table1' => shape(
			'name' => 'table1',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'name_uniq',
					'type' => 'UNIQUE',
					'fields' => keyset['name'],
				),
			],
		),
		'table2' => shape(
			'name' => 'table2',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'table_1_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'table_1_id',
					'type' => 'INDEX',
					'fields' => keyset['table_1_id'],
				),
			],
		),
	],
	'db2' => dict[
		'table3' => shape(
			'name' => 'table3',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'name_uniq',
					'type' => 'UNIQUE',
					'fields' => keyset['name'],
				),
			],
		),
		'table4' => shape(
			'name' => 'table4',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'table_3_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'table_3_id',
					'type' => 'INDEX',
					'fields' => keyset['table_3_id'],
				),
			],
		),
	],

];
