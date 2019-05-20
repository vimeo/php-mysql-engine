<?hh // strict

namespace Slack\DBMock;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class UpdateQueryTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	public async function testUpdateSingleRow(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("UPDATE table3 SET name='updated' WHERE id=1");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'updated'],
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testUpdateMultipleRows(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("UPDATE table3 set name=CONCAT(name, id, 'updated'), group_id = 13 WHERE group_id=6");
		$results = await $conn->query("SELECT * FROM table3 WHERE group_id=13");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 4, 'group_id' => 13, 'name' => 'name34updated'],
			dict['id' => 6, 'group_id' => 13, 'name' => 'name36updated'],
		]);
	}

	public async function testUpdateWithLimit(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("UPDATE table3 set name='updated', group_id = 13 WHERE group_id=6 LIMIT 1");
		$results = await $conn->query("SELECT * FROM table3 WHERE group_id=13");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 4, 'group_id' => 13, 'name' => 'updated'],
		]);
	}

	public async function testUpdateWithLimitAndOrderBy(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("UPDATE table3 set name='updated', group_id = 13 WHERE group_id=6 ORDER BY id desc LIMIT 1");
		$results = await $conn->query("SELECT * FROM table3 WHERE group_id=13");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 6, 'group_id' => 13, 'name' => 'updated'],
		]);

		$results = await $conn->query("SELECT * FROM table3 WHERE id=6");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 6, 'group_id' => 13, 'name' => 'updated'],
		]);
	}

	public async function testQualifiedTable(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict['id' => 4, 'group_id' => 13, 'name' => 'name34updated'],
			dict['id' => 6, 'group_id' => 13, 'name' => 'name36updated'],
		];
		await $conn->query("UPDATE db2.table3 set name=CONCAT(name, id, 'updated'), group_id = 13 WHERE group_id=6");
		$results = await $conn->query("SELECT * FROM table3 WHERE group_id=13");
		expect($results->rows())->toBeSame($expected, 'no backticks');
	}

	public async function testQualifiedTableBackticks(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict['id' => 4, 'group_id' => 13, 'name' => 'name34updated'],
			dict['id' => 6, 'group_id' => 13, 'name' => 'name36updated'],
		];
		await $conn->query(
			"UPDATE `db2`.`table3` set name=CONCAT(name, id, 'updated'), group_id = 13 WHERE group_id=6",
		);
		$results = await $conn->query("SELECT * FROM table3 WHERE group_id=13");
		expect($results->rows())->toBeSame($expected, 'with backticks');
	}

	public async function testPrimaryKeyViolation(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		expect(() ==> $conn->query("UPDATE table3 set id=1"))->toThrow(DBMockUniqueKeyViolation::class);
	}

	public async function testTypeCoercion(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("UPDATE table3 set name=1 WHERE id=6");
		$results = await $conn->query("SELECT * FROM table3 WHERE id=6");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 6, 'group_id' => 6, 'name' => '1'],
			],
		);
	}

	public async function testTypeCoercionStrict(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		QueryContext::$strictMode = true;
		expect(() ==> $conn->query("UPDATE table3 set name=1 WHERE id=6"))->toThrow(
			DBMockRuntimeException::class,
			"Invalid value '1' for column 'name' on 'table3', expected string",
		);
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
