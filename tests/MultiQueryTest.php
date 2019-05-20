<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

final class MultiQueryTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	public async function testSubqueryInSelect(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT * FROM (SELECT id FROM table4 WHERE id = 1001 OR id = 1004) as sub WHERE id = 1001",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001],
		]);
	}

	public async function testSubqueryInSelectAndWhere(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT id, (SELECT description from table4 where id=1004) as foo FROM table3 WHERE id IN (SELECT table_3_id FROM association_table WHERE table_4_id=1003)",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 3, 'foo' => 'desc2'],
		]);
	}

	public async function testUnion(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT * from table3 where id = 1
			UNION SELECT * from table3 WHERE id = 2
			UNION SELECT * from table3 where id = 3
			UNION SELECT * from table3 where id = 3",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			// because UNION dedupes, this row is not duplicated even though it's selected twice
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
		]);
	}

	public async function testUnionAll(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT * from table3 where id = 1
			UNION ALL SELECT * from table3 WHERE id = 2
			UNION ALL SELECT * from table3 where id = 3
			UNION ALL SELECT * from table3 where id = 3",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			// no dedupe with union all
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
		]);
	}

	public async function testIntersect(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT * from table3
			INTERSECT SELECT * from table3 WHERE id = 2",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
		]);
	}

	public async function testExcept(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT * from table3
			EXCEPT SELECT * from table3 WHERE id = 2",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			// no dedupe with union all
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
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
