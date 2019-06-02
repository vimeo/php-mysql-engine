<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class DeleteQueryTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	public async function testDeleteSingleRow(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE FROM table3 WHERE id=1");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testDeleteMultipleRows(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE FROM table3 WHERE group_id=12345");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testDeleteWithLimit(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE FROM table3 WHERE group_id=12345 LIMIT 2");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testDeleteWithLimitAndOrderBy(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE FROM table3 WHERE group_id=12345 ORDER BY id DESC LIMIT 2");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testQualifiedTable(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE FROM db2.table3 WHERE group_id=12345");
		$expected = vec[
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		];
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame($expected, 'with no backticks');

		await $conn->query("DELETE FROM `db2`.`table3` WHERE group_id=12345");
		$expected = vec[
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		];
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame($expected, 'with backticks');

		await $conn->query("DELETE FROM `db2`.table3 WHERE group_id=12345");
		$expected = vec[
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		];
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame($expected, 'with partial backticks because why not');
	}

	public async function testDeleteWithoutFrom(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		await $conn->query("DELETE table3 WHERE id=1");
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
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
		QueryContext::$strictSQLMode = false;
		QueryContext::$strictSchemaMode = false;
	}
}
