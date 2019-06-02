<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

final class SQLFunctionTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	public async function testModAsKeyword(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT * FROM table4 WHERE group_id=12345 AND id MOD 10 = 1");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
		]);
	}

	public async function testModAsFunction(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT * FROM table4 WHERE group_id=12345 AND MOD(id, 10) = 1");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
		]);
	}

	public async function testGroupByWithAggregateFunctions(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict['group_id' => 12345, 'count(*)' => 3],
			dict['group_id' => 0, 'count(*)' => 1],
		];
		$results = await $conn->query("select group_id, count(*) from association_table group by group_id");
		expect($results->rows())->toBeSame($expected, 'with column reference in group_by');

		$results = await $conn->query("select group_id, count(*) from association_table group by 1");
		expect($results->rows())->toBeSame($expected, 'with positional reference in group_by');

		$results =
			await $conn->query("select group_id, count(*) from association_table group by association_table.group_id");
		expect($results->rows())->toBeSame($expected, 'with column and alias reference in group_by');

		$results =
			await $conn->query("select group_id, count(1) from association_table group by association_table.group_id");
		expect($results->rows())->toBeSame(
			vec[dict['group_id' => 12345, 'count(1)' => 3], dict['group_id' => 0, 'count(1)' => 1]],
			'with count(1) instead of count(*)',
		);

		$results = await $conn->query(
			"select group_id, count(table_3_id) from association_table group by association_table.group_id",
		);
		expect($results->rows())->toBeSame(
			vec[dict['group_id' => 12345, 'count(table_3_id)' => 3], dict['group_id' => 0, 'count(table_3_id)' => 1]],
			'with count(table_3_id) instead of count(*)',
		);
	}

	public async function testCountDistinct(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results =
			await $conn->query("select group_id, count(DISTINCT table_3_id) from association_table group by group_id");
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 12345, 'count(DISTINCT table_3_id)' => 2],
				dict['group_id' => 0, 'count(DISTINCT table_3_id)' => 1],
			],
		);
	}

	public async function testCountNullable(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT id, COUNT(table_4_id) thecount FROM table3 LEFT OUTER JOIN association_table ON id=table_3_id GROUP BY id",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'thecount' => 2],
			dict['id' => 2, 'thecount' => 1],
			dict['id' => 3, 'thecount' => 1],
			dict['id' => 4, 'thecount' => 0],
			dict['id' => 6, 'thecount' => 0],
		]);
	}

	public async function testGroupByNullable(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT c.id as table_4_id, COUNT(table_3_id) as thecount FROM table4 c LEFT JOIN association_table s ON c.id = s.table_4_id GROUP BY c.id",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['table_4_id' => 1000, 'thecount' => 2],
				dict['table_4_id' => 1001, 'thecount' => 1],
				dict['table_4_id' => 1002, 'thecount' => 0],
				dict['table_4_id' => 1003, 'thecount' => 1],
				dict['table_4_id' => 1004, 'thecount' => 0],
			],
		);
	}

	public async function testSum(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("select group_id, SUM(table_3_id) from association_table group by group_id");
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 12345, 'SUM(table_3_id)' => 4],
				dict['group_id' => 0, 'SUM(table_3_id)' => 3],
			],
		);
	}

	public async function testMinMax(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT group_id, MIN(table_4_id), MAX(table_4_id) FROM association_table GROUP BY group_id",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict[
					'group_id' => 12345,
					'MIN(table_4_id)' => 1000,
					'MAX(table_4_id)' => 1001,
				],
				dict[
					'group_id' => 0,
					'MIN(table_4_id)' => 1003,
					'MAX(table_4_id)' => 1003,
				],
			],
		);
	}

	public async function testAggNoGroupBy(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT COUNT(*), MIN(table_4_id), MAX(table_4_id), AVG(table_4_id) FROM association_table",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict[
					'COUNT(*)' => 4,
					'MIN(table_4_id)' => 1000,
					'MAX(table_4_id)' => 1003,
					'AVG(table_4_id)' => 1001.0,
				],
			],
		);
	}

	public async function testSubstr(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT SUBSTR('foobar', 1, 2) as first, SUBSTR('foobar', 2) as second, SUBSTR('foobar', 3, 2) as third",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['first' => 'fo', 'second' => 'oobar', 'third' => 'ob'],
			],
			'ASCII',
		);

		$results = await $conn->query(
			"SELECT SUBSTR('よかったですね', 1, 2) as first, SUBSTR('よかったですね', 2) as second, SUBSTR('よかったですね', 3, 2) as third",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['first' => 'よか', 'second' => 'かったですね', 'third' => 'った'],
			],
			'Utf8',
		);
	}

	public async function testLength(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT LENGTH('foobar')");
		expect($results->rows())->toBeSame(
			vec[
				dict["LENGTH('foobar')" => 6],
			],
			'ASCII',
		);

		$results = await $conn->query("SELECT LENGTH('よかったですね')");
		expect($results->rows())->toBeSame(
			vec[
				dict["LENGTH('よかったですね')" => 21],
			],
			'Utf8',
		);
	}

	public async function testCharLength(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT CHAR_LENGTH('foobar')");
		expect($results->rows())->toBeSame(
			vec[
				dict["CHAR_LENGTH('foobar')" => 6],
			],
			'ASCII',
		);

		$results = await $conn->query("SELECT CHAR_LENGTH('よかったですね')");
		expect($results->rows())->toBeSame(
			vec[
				dict["CHAR_LENGTH('よかったですね')" => 7],
			],
			'Utf8',
		);
	}

	public async function testCoalesce(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT COALESCE(1, 2, 3) as first, COALESCE(NULL, 2) as second, COALESCE(NULL, NULL, NULL) as third",
		);
		expect($results->rows())->toBeSame(vec[
			dict['first' => 1, 'second' => 2, 'third' => null],
		]);
	}

	public async function testGreatest(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT GREATEST(1, 3, 2) as first, GREATEST(NULL, 2) as second, GREATEST(NULL, NULL, NULL) as third",
		);
		expect($results->rows())->toBeSame(vec[
			dict['first' => 3, 'second' => 2, 'third' => null],
		]);
	}

	public async function testNullif(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT NULLIF(1, 2) as first, NULLIF(1, 1) as second");
		expect($results->rows())->toBeSame(vec[
			dict['first' => 1, 'second' => null],
		]);
	}

	public async function testConcatWS(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"select concat_ws('-',group_id, table_3_id) as concat_key from association_table where group_id='12345'",
		);
		expect($results->rows())->toBeSame(vec[
			dict['concat_key' => "12345-1"],
			dict['concat_key' => "12345-1"],
			dict['concat_key' => "12345-2"],
		]);
	}

	public async function testField(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("select id FROM table4 ORDER BY field(id, 1002, 1001, 1004, 1003)");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1000],
			dict['id' => 1002],
			dict['id' => 1001],
			dict['id' => 1004],
			dict['id' => 1003],
		]);
	}

	public async function testIf(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query('SELECT id, IF(id >= 4, 1, 0) as test FROM table3');
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'test' => 0],
			dict['id' => 2, 'test' => 0],
			dict['id' => 3, 'test' => 0],
			dict['id' => 4, 'test' => 1],
			dict['id' => 6, 'test' => 1],
		]);
	}

	<<__Override>>
	public static async function beforeFirstTestAsync(): Awaitable<void> {
		static::$conn = await SharedSetup::initAsync();
		// block hole logging
		Logger::setHandle(new \Facebook\CLILib\TestLib\StringOutput());
	}

	<<__Override>>
	public async function beforeEachTestAsync(): Awaitable<void> {
		restore('setup');
		QueryContext::$strictSchemaMode = false;
		QueryContext::$strictSQLMode = false;
	}
}
