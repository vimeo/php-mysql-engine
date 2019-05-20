<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

final class SelectClauseTest extends HackTest {

	private static ?AsyncMysqlConnection $conn;

	<<__Override>>
	public static async function beforeFirstTestAsync(): Awaitable<void> {
		static::$conn = await SharedSetup::initAsync();
	}

	<<__Override>>
	public async function beforeEachTestAsync(): Awaitable<void> {
		restore('setup');
		QueryContext::$strictMode = false;
	}

	public async function testNoFromClause(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT 1 as foo");
		expect($results->rows())->toBeSame(
			vec[
				dict['foo' => 1],
			],
		);
	}

	public async function testWhereClauseAndAliases(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id, group_id as my_fav_group_id FROM table3 WHERE group_id=6");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6],
				dict['id' => 6, 'my_fav_group_id' => 6],
			],
		);
	}

	public async function testDatabaseName(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id, group_id as my_fav_group_id FROM db2.table3 WHERE group_id=6");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6],
				dict['id' => 6, 'my_fav_group_id' => 6],
			],
		);

		$results = await $conn->query("SELECT id, group_id as my_fav_group_id FROM `db2`.`table3` WHERE group_id=6");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6],
				dict['id' => 6, 'my_fav_group_id' => 6],
			],
			'with backtick quoted identifiers',
		);


		$results = await $conn->query(
			"SELECT table3.id, table3.group_id as my_fav_group_id FROM `db2`.`table3` WHERE group_id=6",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6],
				dict['id' => 6, 'my_fav_group_id' => 6],
			],
			'column identifiers',
		);

		/*
		TODO: this doesn't currently work
		$results = await $conn->query(
			"SELECT db2.table3.id, db2.table3.group_id as my_fav_group_id FROM `db2`.`table3` WHERE group_id=6",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6],
				dict['id' => 6, 'my_fav_group_id' => 6],
			],
			'column identifiers',
		);
		*/
	}

	public async function testNonexistentColumnInWhere(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND doesnotexist='name2'");
		expect($results->rows())->toBeSame(vec[]);
	}

	public async function testNonexistentColumnInWhereStrict(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		QueryContext::$strictMode = true;
		expect(() ==> $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND doesnotexist='name2'"))->toThrow(
			SQLFakeRuntimeException::class,
			"Column with index doesnotexist not found in row",
		);
	}

	public async function testHaving(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT group_id, COUNT(*) FROM association_table GROUP BY group_id HAVING COUNT(*) > 2 AND group_id > 1",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 12345, 'COUNT(*)' => 3],
			],
		);
	}

	public async function testOrderBy(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC");
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 0, 'table_4_id' => 1003],
				dict['group_id' => 12345, 'table_4_id' => 1001],
				dict['group_id' => 12345, 'table_4_id' => 1000],
				dict['group_id' => 12345, 'table_4_id' => 1000],
			],
		);
	}

	public async function testOrderByNotInSelect(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT table_4_id FROM association_table WHERE table_4_id IN (1001, 1003) ORDER BY group_id DESC",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['table_4_id' => 1001],
				dict['table_4_id' => 1003],
			],
			'results sorted by group_id ascending even though group_id is not in select list',
		);

		$results = await $conn->query(
			"SELECT table_4_id FROM association_table WHERE table_4_id IN (1001, 1003) ORDER BY group_id ASC",
		);
		expect($results->rows())->toBeSame(
			vec[
				# by group_id ascending
				dict['table_4_id' => 1003],
				dict['table_4_id' => 1001],
			],
			'results sorted by group_id ascending even though group_id is not in select list',
		);
	}

	public async function testOrderByWithAlias(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT s.* FROM association_table s ORDER BY s.table_4_id, s.table_3_id ASC");
		expect($results->rows())->toBeSame(
			vec[
				dict['table_3_id' => 1, 'table_4_id' => 1000, 'description' => 'association 1', 'group_id' => 12345],
				dict['table_3_id' => 2, 'table_4_id' => 1000, 'description' => 'association 3', 'group_id' => 12345],
				dict['table_3_id' => 1, 'table_4_id' => 1001, 'description' => 'association 2', 'group_id' => 12345],
				dict['table_3_id' => 3, 'table_4_id' => 1003, 'description' => 'association 4', 'group_id' => 0],
			],
		);
	}

	public async function testOrderByWithSelectAlias(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT table_4_id AS other_id FROM association_table ORDER BY other_id DESC");
		expect($results->rows())->toBeSame(
			vec[
				dict['other_id' => 1003],
				dict['other_id' => 1001],
				dict['other_id' => 1000],
				dict['other_id' => 1000],
			],
		);
	}

	public async function testLimit(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results =
			await $conn->query("select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC LIMIT 2");
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 0, 'table_4_id' => 1003],
				dict['group_id' => 12345, 'table_4_id' => 1001],
			],
		);
	}

	public async function testLimitOffset(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC LIMIT 2 OFFSET 1",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 12345, 'table_4_id' => 1001],
				dict['group_id' => 12345, 'table_4_id' => 1000],
			],
		);
	}

	public async function testSkipLockHint(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict['id' => 4, 'my_fav_group_id' => 6],
			dict['id' => 6, 'my_fav_group_id' => 6],
		];
		$results = await $conn->query("SELECT id, group_id as my_fav_group_id
		FROM table3 WHERE group_id=6 FOR UPDATE");
		expect($results->rows())->toBeSame($expected, 'FOR UPDATE');

		$results = await $conn->query("SELECT id, group_id as my_fav_group_id
		FROM table3 WHERE group_id=6 LOCK IN SHARE MODE");
		expect($results->rows())->toBeSame($expected, 'LOCK IN SHARE MODE');
	}

	public async function testOutOfOrderClauses(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		expect(
			() ==> $conn->query("select group_id, table_4_id from association_table LIMIT 2 ORDER BY group_id, 2 DESC"),
		)
			->toThrow(SQLFakeParseException::class, "Unexpected ORDER in SQL query");
	}
}
