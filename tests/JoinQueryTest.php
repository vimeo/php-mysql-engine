<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;
use namespace HH\Lib\Vec;

final class JoinQueryTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	public async function testInnerJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict[
				'id' => 1,
				'group_id' => 12345,
				'name' => 'name1',
				'table_3_id' => 1,
				'table_4_id' => 1000,
				'description' => 'association 1',
			],
			dict[
				'id' => 1,
				'group_id' => 12345,
				'name' => 'name1',
				'table_3_id' => 1,
				'table_4_id' => 1001,
				'description' => 'association 2',
			],
			dict[
				'id' => 2,
				'group_id' => 12345,
				'name' => 'name2',
				'table_3_id' => 2,
				'table_4_id' => 1000,
				'description' => 'association 3',
			],
			dict[
				'id' => 3,
				'group_id' => 12345,
				'name' => 'name3',
				'table_3_id' => 3,
				'table_4_id' => 1003,
				'description' => 'association 4',
			],
		];

		$results = await $conn->query("SELECT * FROM table3 JOIN association_table ON id = table_3_id");
		expect($results->rows())->toBeSame($expected, 'with no aliases and column names inferred from table schema');

		$results = await $conn->query(
			"SELECT * FROM table3 JOIN association_table ON table3.id = association_table.table_3_id",
		);
		expect($results->rows())->toBeSame($expected, 'with columns using explicitly specified table names');

		$results = await $conn->query(
			"SELECT * FROM table3 f JOIN association_table s ON f.id = s.table_3_id AND f.group_id = s.group_id",
		);

		# there is (intentionally) one row here where the group_ids don't match, so we should see that filtered out here too
		$expected = Vec\slice($expected, 0, 3);
		expect($results->rows())->toBeSame($expected, 'with aliases and explicit group_id filter');
	}

	public async function testInnerJoinThreeTables(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT table3.id as table_3_id, table4.description as descr, table4.group_id FROM table3 JOIN association_table on id=table_3_id JOIN table4 ON table_4_id = table4.id",
		);
		expect($results->rows())->toBeSame(vec[
			dict['table_3_id' => 1, 'descr' => 'desc1', 'group_id' => 12345],
			dict['table_3_id' => 1, 'descr' => 'desc2', 'group_id' => 12345],
			dict['table_3_id' => 2, 'descr' => 'desc1', 'group_id' => 12345],
			dict['table_3_id' => 3, 'descr' => 'desc1', 'group_id' => 7],
		]);
	}

	public async function testStraightJoinThreeTables(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT table3.id as table_3_id, table4.description as descr, table4.group_id FROM table3 STRAIGHT_JOIN association_table on id=table_3_id STRAIGHT_JOIN table4 ON table_4_id = table4.id",
		);
		expect($results->rows())->toBeSame(vec[
			dict['table_3_id' => 1, 'descr' => 'desc1', 'group_id' => 12345],
			dict['table_3_id' => 1, 'descr' => 'desc2', 'group_id' => 12345],
			dict['table_3_id' => 2, 'descr' => 'desc1', 'group_id' => 12345],
			dict['table_3_id' => 3, 'descr' => 'desc1', 'group_id' => 7],
		]);
	}

	public async function testLeftJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results =
			await $conn->query("SELECT id, table_4_id FROM table3 LEFT OUTER JOIN association_table ON id=table_3_id");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'table_4_id' => 1000],
			dict['id' => 1, 'table_4_id' => 1001],
			dict['id' => 2, 'table_4_id' => 1000],
			dict['id' => 3, 'table_4_id' => 1003],
			dict['id' => 4, 'table_4_id' => null],
			dict['id' => 6, 'table_4_id' => null],
		]);
	}

	public async function testRightJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT table_3_id, table4.id as table_4_id FROM association_table RIGHT OUTER JOIN table4 ON id=table_4_id",
		);
		expect($results->rows())->toBeSame(vec[
			dict['table_3_id' => 1, 'table_4_id' => 1000],
			dict['table_3_id' => 2, 'table_4_id' => 1000],
			dict['table_3_id' => 1, 'table_4_id' => 1001],
			dict['table_3_id' => null, 'table_4_id' => 1002],
			dict['table_3_id' => 3, 'table_4_id' => 1003],
			dict['table_3_id' => null, 'table_4_id' => 1004],
		]);
	}

	public async function testCrossJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT table_3_id, id as table_4_id FROM association_table, table4 WHERE table4.id=1003",
		);
		expect($results->rows())->toBeSame(vec[
			dict['table_3_id' => 1, 'table_4_id' => 1003],
			dict['table_3_id' => 1, 'table_4_id' => 1003],
			dict['table_3_id' => 2, 'table_4_id' => 1003],
			dict['table_3_id' => 3, 'table_4_id' => 1003],
		]);
	}

	public async function testNaturalJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id, table_4_id FROM table3 NATURAL JOIN association_table");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'table_4_id' => 1000],
			dict['id' => 1, 'table_4_id' => 1001],
			dict['id' => 1, 'table_4_id' => 1000],
			dict['id' => 2, 'table_4_id' => 1000],
			dict['id' => 2, 'table_4_id' => 1001],
			dict['id' => 2, 'table_4_id' => 1000],
			dict['id' => 3, 'table_4_id' => 1000],
			dict['id' => 3, 'table_4_id' => 1001],
			dict['id' => 3, 'table_4_id' => 1000],
		]);
	}

	<<__Override>>
	public static async function beforeFirstTestAsync(): Awaitable<void> {
		static::$conn = await SharedSetup::initAsync();
	}

	<<__Override>>
	public async function beforeEachTestAsync(): Awaitable<void> {
		restore('setup');
		QueryContext::$strictMode = false;
	}
}
