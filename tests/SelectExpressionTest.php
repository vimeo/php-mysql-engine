<?hh // strict

namespace Slack\DBMock;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

final class SelectExpressionTest extends HackTest {

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

	public async function testSelectStar(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT * FROM table3");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		]);
	}

	public async function testSelectExpressions(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results =
			await $conn->query("SELECT id, group_id as my_fav_group_id, id*1000 as math FROM table3 WHERE group_id=6");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 4, 'my_fav_group_id' => 6, 'math' => 4000],
				dict['id' => 6, 'my_fav_group_id' => 6, 'math' => 6000],
			],
		);
	}

	public async function testBinaryOperators(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND name='name2'");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 2],
			],
		);
	}

	public async function testBetween(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table4 WHERE id BETWEEN 1000 AND 1003");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1000],
			dict['id' => 1001],
			dict['id' => 1002],
			dict['id' => 1003],
		]);
	}

	public async function testIn(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table4 WHERE id IN (1000, 1002, 1003)");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1000],
			dict['id' => 1002],
			dict['id' => 1003],
		]);
	}

	public async function testNotIn(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table4 WHERE id NOT IN (1000, 1002, 1003)");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001],
			dict['id' => 1004],
		]);
	}

	public async function testOr(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table4 WHERE id = 1001 OR id = 1004");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001],
			dict['id' => 1004],
		]);
	}

	public async function testComplexCaseExpression(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT id, CASE WHEN id % 4 = 0 THEN 'yep' ELSE 'nope' END foo FROM table4 WHERE id = 1001 OR id = 1004",
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1001, 'foo' => 'nope'],
			dict['id' => 1004, 'foo' => 'yep'],
		]);
	}

	public async function testDistinct(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT DISTINCT group_id FROM table3");
		expect($results->rows())->toBeSame(
			vec[
				dict['group_id' => 12345],
				dict['group_id' => 6],
			],
		);
	}

	public async function testLike(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT 'foo' LIKE 'foo' as test1, 'foobarbaz' like '%bar%' test2, 'foobaz' LIKE 'foo_az' test3, 'foobarqux' like 'foo%' test4, 'foobarqux' LIKE '%qux' test5, 'blahfoobarbazqux blah' LIKE '%foo%qux%' test6, 'blegh' LIKE '%foo%' test7",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['test1' => 1, 'test2' => 1, 'test3' => 1, 'test4' => 1, 'test5' => 1, 'test6' => 1, 'test7' => 0],
			],
		);
	}

	public async function testNotLike(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			"SELECT 'foo' NOT LIKE 'foo' test1, 'foobarbaz' NOT like '%bar%' test2, 'foobaz' NOT LIKE 'foo_az' test3, 'foobarqux' NOT like 'foo%' test4, 'foobarqux' NOT LIKE '%qux' test5, 'blahfoobarbazqux blah' NOT LIKE '%foo%qux%' test6, 'blegh' NOT LIKE '%foo%' test7",
		);
		expect($results->rows())->toBeSame(
			vec[
				dict['test1' => 0, 'test2' => 0, 'test3' => 0, 'test4' => 0, 'test5' => 0, 'test6' => 0, 'test7' => 1],
			],
		);
	}
	public async function testRegexp(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT * FROM table3 WHERE name REGEXP '[A-Z]2'");
		$expected = vec[
			dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
		];
		expect($results->rows())->toBeSame($expected, 'case insensitive regexp');
		$results = await $conn->query("SELECT * FROM table3 WHERE name REGEXP BINARY('[a-z]2')");
		expect($results->rows())->toBeSame($expected, 'regexp binary (case sensitive)');
		$results = await $conn->query("SELECT * FROM table3 WHERE name REGEXP BINARY('[A-Z]2')");
		expect($results->rows())->toBeSame(vec[], 'regexp binary (case sensitive)');
	}

	public async function testNotRegexp(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT * FROM table3 WHERE name NOT REGEXP '[a-z]2'");
		$expected = vec[
			dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
			dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
			dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
			dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
		];
		expect($results->rows())->toBeSame($expected, 'not regexp');
		$results = await $conn->query("SELECT * FROM table3 WHERE name NOT REGEXP BINARY('[a-z]2')");
		expect($results->rows())->toBeSame($expected, 'not regexp binary');
	}

	public async function testUnescape(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT '\\\\foo\'sbar\%\Zbaz\/' as testescape");
		expect($results->rows())->toBeSame(vec[
			dict['testescape' => "\\foo'sbar\%\Zbaz/"],
		]);
	}

	public async function testDoubleQuotes(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query('SELECT id FROM table3 WHERE name="name1"');
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1],
		]);
	}

	public async function testIsNull(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			'SELECT id from table3
			LEFT JOIN association_table ON id = table_3_id WHERE table_3_id IS NULL',
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 4],
			dict['id' => 6],
		]);
	}

	public async function testIsNotNull(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query(
			'SELECT id from table3
			LEFT JOIN association_table ON id = table_3_id WHERE table_3_id IS NOT NULL',
		);
		expect($results->rows())->toBeSame(vec[
			dict['id' => 1],
			// this is duplicated because two associations exist, as MySQL would return
			dict['id' => 1],
			dict['id' => 2],
			dict['id' => 3],
		]);
	}


	public async function testNotParens(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results =
			await $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND NOT (name='name1' OR name='name3')");
		expect($results->rows())->toBeSame(vec[
			dict['id' => 2],
		]);
	}

	public async function testRowComparator(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict['id' => 4, 'my_fav_group_id' => 6],
			dict['id' => 6, 'my_fav_group_id' => 6],
		];
		$results = await $conn->query("SELECT (1, 2, 3) > (4, 5, 6)");
		expect($results->rows())->toBeSame(vec[dict['(1, 2, 3) > (4, 5, 6)' => 0]], 'greater than increasing');

		$results = await $conn->query("SELECT (1, 2, 3) < (4, 5, 6)");
		expect($results->rows())->toBeSame(vec[dict['(1, 2, 3) < (4, 5, 6)' => 1]], 'less than increasing');

		$results = await $conn->query("SELECT (1, 2, 3) > (1, 2, 2)");
		expect($results->rows())->toBeSame(
			vec[dict['(1, 2, 3) > (1, 2, 2)' => 1]],
			'greater than with some elements equal',
		);

		$results = await $conn->query("SELECT (1, 2, 3) > (1, 1, 4)");
		expect($results->rows())->toBeSame(
			vec[dict['(1, 2, 3) > (1, 1, 4)' => 1]],
			'greater than with first element equal',
		);

		$results = await $conn->query("SELECT (4, 5, 6) > (1, 2, 3)");
		expect($results->rows())->toBeSame(vec[dict['(4, 5, 6) > (1, 2, 3)' => 1]], 'greater than decreasing');

		$results = await $conn->query("select * from table3 WHERE (id, group_id) > (3, 1)");
		expect($results->rows())->toBeSame(
			vec[
				dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
				dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
				dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
			],
			'with actual rows',
		);
	}
}
