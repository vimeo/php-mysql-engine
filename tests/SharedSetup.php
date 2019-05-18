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
		'table_with_more_fields' => shape(
			'name' => 'table_with_more_fields',
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
				shape(
					'name' => 'nullable_unique',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => true,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'nullable_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => true,
					'hack_type' => 'int',
					'default' => '1',
				),
				shape(
					'name' => 'not_null_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
					'default' => '2',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id', 'name'],
				),
				shape(
					'name' => 'nullable_unique',
					'type' => 'UNIQUE',
					'fields' => keyset['nullable_unique'],
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
					'name' => 'group_id',
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
				shape(
					'name' => 'group_id',
					'type' => 'INDEX',
					'fields' => keyset['group_id'],
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
					'name' => 'group_id',
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
					'name' => 'group_id',
					'type' => 'INDEX',
					'fields' => keyset['group_id'],
				),
			],
		),
		'association_table' => shape(
			'name' => 'association_table',
			'fields' => vec[
				shape(
					'name' => 'table_3_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'table_4_id',
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
				shape(
					'name' => 'group_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['table_3_id', 'table_4_id'],
				),
				shape(
					'name' => 'table_4_id',
					'type' => 'INDEX',
					'fields' => keyset['table_4_id'],
				),
			],
		),
	],

];
