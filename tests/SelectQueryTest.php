<?hh // strict

namespace Slack\DBMock;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\{DataProvider, HackTest};
use namespace HH\Lib\{Str, Vec};

final class SelectQueryTest extends HackTest {

	private static ?AsyncMysqlConnection $conn;

	public static async function beforeFirstTestAsync(): Awaitable<void> {
		init(TEST_SCHEMA, true);
		$pool = new AsyncMysqlConnectionPool(darray[]);
		$conn = await $pool->connect("example", 1, 'db2', '', '');

		// populate database state
		$database = dict[
			'table3' => vec[
				dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
				dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
				dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
				dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
				dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
			],
			'table4' => vec[
				dict['id' => 1000, 'group_id' => 12345, 'description' => 'desc1'],
				dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
				dict['id' => 1002, 'group_id' => 12345, 'description' => 'desc3'],
				dict['id' => 1003, 'group_id' => 7, 'description' => 'desc1'],
				dict['id' => 1004, 'group_id' => 7, 'description' => 'desc2'],
			],
			'association_table' => vec[
				dict['table_3_id' => 1, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'assocation 1'],
				dict['table_3_id' => 1, 'table_4_id' => 1001, 'group_id' => 12345, 'description' => 'assocation 2'],
				dict['table_3_id' => 2, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'assocation 3'],
				dict['table_3_id' => 3, 'table_4_id' => 1003, 'group_id' => 0, 'description' => 'assocation 4'],
			],
		];
		$conn->getServer()->databases['db2'] = $database;
		static::$conn = $conn;
		snapshot('setup');
	}

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

	public async function testNonexistentColumnInWhere(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND mimetype='name2'");
		expect($results->rows())->toBeSame(vec[]);
	}

	public async function testNonexistentColumnInWhereStrict(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		QueryContext::$strictMode = true;
		expect(() ==> $conn->query("SELECT id FROM table3 WHERE group_id=12345 AND doesnotexist='name2'"))->toThrow(
			DBMockRuntimeException::class,
			"Column with index doesnotexist not found in row",
		);
	}

	public async function testInnerJoin(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$expected = vec[
			dict[
				'id' => 1,
				'group_id' => 12345,
				'name' => 'name1',
				'table_3_id' => 1,
				'table_4_id' => 1000,
				'description' => 'assocation 1',
			],
			dict[
				'id' => 1,
				'group_id' => 12345,
				'name' => 'name1',
				'table_3_id' => 1,
				'table_4_id' => 1001,
				'description' => 'assocation 2',
			],
			dict[
				'id' => 2,
				'group_id' => 12345,
				'name' => 'name2',
				'table_3_id' => 2,
				'table_4_id' => 1000,
				'description' => 'assocation 3',
			],
			dict[
				'id' => 3,
				'group_id' => 12345,
				'name' => 'name3',
				'table_3_id' => 3,
				'table_4_id' => 1003,
				'description' => 'assocation 4',
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

	public async function testGroupByWithAggregateFunctions(): Awaitable<void> {
		$conn = static::$conn as nonnull;
		$results = await $conn->query("select group_id, count(*) from association_table group by group_id");
		expect($results->rows())->toBeSame(vec[
			dict['group_id' => 12345, 'count(*)' => 3],
			dict['group_id' => 7, 'count(*)' => 1],
		]);
	}


	/*

				it(
					'handles group by with aggregate functions',
					() ==> {
						$ret = local_query_run("select group_id, count(*) from association_table group by group_id");
						$expected = array(
							array('group_id' => 12345, 'count(*)' => 3),
							array('group_id' => 7, 'count(*)' => 1),
						);

						expect($ret)->toBeSame($expected, 'with column reference in group_by');

						$ret = local_query_run("select group_id, count(*) from association_table group by 1");
						expect($ret)->toBeSame($expected, 'with positional reference in group_by');

						$ret = local_query_run("select group_id, count(*) from association_table group by association_table.group_id");
						expect($ret)->toBeSame($expected, 'with column and alias reference in group_by');

						$expected = array(
							array('group_id' => 12345, 'count(1)' => 3),
							array('group_id' => 7, 'count(1)' => 1),
						);

						$ret = local_query_run("select group_id, count(1) from association_table group by association_table.group_id");
						expect($ret)->toBeSame($expected, 'with count(1) instead of count(*)');

						$expected = array(
							array('group_id' => 12345, 'count(table_3_id)' => 3),
							array('group_id' => 7, 'count(table_3_id)' => 1),
						);

						$ret = local_query_run("select group_id, count(table_3_id) from association_table group by association_table.group_id");
						expect($ret)->toBeSame($expected, 'with count(table_3_id) instead of count(*)');
					},
				);

				it(
					'handles group by with count(distinct columnname)',
					() ==> {
						$ret = local_query_run("select group_id, count(DISTINCT table_3_id) from association_table group by group_id");
						$expected = array(
							array('group_id' => 12345, 'count(DISTINCT table_3_id)' => 2),
							array('group_id' => 7, 'count(DISTINCT table_3_id)' => 1),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles group by with count on a nullable field',
					() ==> {
						$ret = local_query_run(
							"SELECT c.id as table_4_id, COUNT(table_3_id) as file_count FROM table4 c LEFT JOIN association_table s ON c.id = s.table_4_id GROUP BY c.id",
						);
						$expected = array(
							array('table_4_id' => 1000, 'file_count' => 2),
							array('table_4_id' => 1001, 'file_count' => 1),
							array('table_4_id' => 1002, 'file_count' => 0),
							array('table_4_id' => 1003, 'file_count' => 1),
							array('table_4_id' => 1004, 'file_count' => 0),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'does not throw an error even if column does not exist if we do not have the schema',
					() ==> {
						$ret = local_query_run("select i_am_a_little_teapot from association_table limit 1");
						$expected = array(
							array('i_am_a_little_teapot' => null),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'throws an error if column does not exist in schema',
					() ==> {
						$database = Map {};
						$database['teams'] = Vector {
							array('id' => 1000, 'name' => 'murp'),
						};

						expect(
							() ==> {
								db_mock_query_select("vitess", "SELECT i_am_a_little_teapot FROM teams LIMIT 1", $database);
							},
						)->toThrow(Exception::class);
					},
				);

				it(
					'handles SUM',
					() ==> {
						$ret = local_query_run("select group_id, SUM(table_3_id) from association_table group by group_id");
						$expected = array(
							array('group_id' => 12345, 'SUM(table_3_id)' => 4),
							array('group_id' => 7, 'SUM(table_3_id)' => 3),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles MIN(), MAX(), AVG()',
					() ==> {
						$ret = local_query_run("SELECT group_id, MIN(table_4_id), MAX(table_4_id), avg(table_4_id) FROM association_table GROUP BY group_id");
						$expected = array(
							array('group_id' => 12345, 'MIN(table_4_id)' => 1000, 'MAX(table_4_id)' => 1001, 'avg(table_4_id)' => (1000 + 1001 + 1000) / 3),
							array('group_id' => 7, 'MIN(table_4_id)' => 1003, 'MAX(table_4_id)' => 1003, 'avg(table_4_id)' => 1003),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles aggregate functions with no explicit GROUP BY',
					() ==> {
						$ret = local_query_run("SELECT COUNT(*), MIN(table_4_id), MAX(table_4_id), AVG(table_4_id) FROM association_table");
						$expected = array(
							array('COUNT(*)' => 4, 'MIN(table_4_id)' => 1000, 'MAX(table_4_id)' => 1003, 'AVG(table_4_id)' => 1001),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles DISTINCT',
					() ==> {
						$ret = local_query_run("SELECT DISTINCT group_id FROM table3");
						$expected = array(
							array('group_id' => 12345),
							array('group_id' => 6),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles HAVING with both aggregate functions and column expressions',
					() ==> {
						$ret = local_query_run("SELECT group_id, COUNT(*) FROM association_table GROUP BY group_id HAVING COUNT(*) > 2 AND group_id > 1");
						$expected = array(
							array('group_id' => 12345, 'COUNT(*)' => 3),
						);

						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles ORDER BY',
					() ==> {
						$ret = local_query_run("select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC");
						$expected = array(
							array('group_id' => 7, 'table_4_id' => 1003),
							array('group_id' => 12345, 'table_4_id' => 1001),
							array('group_id' => 12345, 'table_4_id' => 1000),
							array('group_id' => 12345, 'table_4_id' => 1000),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles ORDER BY when the field is not in the SELECT',
					() ==> {
						$ret = local_query_run("SELECT table_4_id FROM association_table WHERE table_4_id IN (1001, 1003) ORDER BY group_id DESC");
						# channel 1001 -> team 12345
						# channel 1003 -> team 7
						# should be sorted by group_id descending, so channel id 1001 then 1003
						$expected = array(
							array('table_4_id' => 1001),
							array('table_4_id' => 1003),
						);
						expect($ret)->toBeSame($expected);

						$ret = local_query_run("SELECT table_4_id FROM association_table WHERE table_4_id IN (1001, 1003) ORDER BY group_id ASC");
						# channel 1003 -> team 7
						# channel 1001 -> team 12345
						# should be sorted by group_id ascending, so channel id 1003 then 1001
						$expected = array(
							array('table_4_id' => 1003),
							array('table_4_id' => 1001),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles ORDER BY with an alias',
					() ==> {
						$ret = local_query_run("SELECT s.* FROM association_table s ORDER BY s.table_4_id, s.table_3_id ASC");
						$expected = array(
							array('group_id' => 12345, 'table_3_id' => 1, 'table_4_id' => 1000),
							array('group_id' => 12345, 'table_3_id' => 2, 'table_4_id' => 1000),
							array('group_id' => 12345, 'table_3_id' => 1, 'table_4_id' => 1001),
							array('group_id' => 7, 'table_3_id' => 3, 'table_4_id' => 1003),
						);

						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles ORDER BY with a select alias',
					() ==> {
						$ret = local_query_run("SELECT table_4_id AS other_id FROM association_table ORDER BY other_id DESC");
						$expected = array(
							array('other_id' => 1003),
							array('other_id' => 1001),
							array('other_id' => 1000),
							array('other_id' => 1000),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'errors when clauses are out of order',
					() ==> {
						$sql = "select group_id, table_4_id from association_table LIMIT 2 ORDER BY group_id, 2 DESC";
						try {
							$ret = local_query_run($sql);
							expect(true)->toBeSame(false, 'should not get here');
						} catch (Exception $e) {
							expect($e->getMessage())->toBeSame("DB Mock parse error: Unexpected ORDER in SQL query: $sql");
						}
					},
				);

				it(
					'handles LIMIT',
					() ==> {
						$ret = local_query_run("select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC LIMIT 2");
						$expected = array(
							array('group_id' => 7, 'table_4_id' => 1003),
							array('group_id' => 12345, 'table_4_id' => 1001),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles LIMIT with OFFSET',
					() ==> {
						$ret = local_query_run("select group_id, table_4_id from association_table ORDER BY group_id, 2 DESC LIMIT 2 OFFSET 1");
						$expected = array(
							array('group_id' => 12345, 'table_4_id' => 1001),
							array('group_id' => 12345, 'table_4_id' => 1000),
						);
						expect($ret)->toBeSame($expected);
					},
				);


				it(
					'handles LIKE and NOT LIKE',
					() ==> {
						$ret = local_query_run(
							"SELECT 'foo' LIKE 'foo' as test1, 'foobarbaz' like '%bar%' test2, 'foobaz' LIKE 'foo_az' test3, 'foobarqux' like 'foo%' test4, 'foobarqux' LIKE '%qux' test5, 'blahfoobarbazqux blah' LIKE '%foo%qux%' test6, 'blegh' LIKE '%foo%' test7",
						);
						$expected = array(
							array('test1' => 1, 'test2' => 1, 'test3' => 1, 'test4' => 1, 'test5' => 1, 'test6' => 1, 'test7' => 0),
						);

						expect($ret)->toBeSame($expected, 'handles LIKE');
						$ret = local_query_run(
							"SELECT 'foo' NOT LIKE 'foo' test1, 'foobarbaz' NOT like '%bar%' test2, 'foobaz' NOT LIKE 'foo_az' test3, 'foobarqux' NOT like 'foo%' test4, 'foobarqux' NOT LIKE '%qux' test5, 'blahfoobarbazqux blah' NOT LIKE '%foo%qux%' test6, 'blegh' NOT LIKE '%foo%' test7",
						);

						$expected = array(
							array('test1' => 0, 'test2' => 0, 'test3' => 0, 'test4' => 0, 'test5' => 0, 'test6' => 0, 'test7' => 1),
						);

						expect($ret)->toBeSame($expected, 'handles NOT LIKE');
					},
				);

				it(
					'handles REGEXP',
					() ==> {
						# different case
						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype REGEXP 'jso[A-Z]'");
						$expected = array(
							['id' => 2, 'group_id' => 12345, 'mimetype' => 'name2'],
						);

						expect($ret)->toBeSame($expected, 'handles REGEXP');

						# same case
						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype REGEXP 'jso[a-z]'");
						$expected = array(
							['id' => 2, 'group_id' => 12345, 'mimetype' => 'name2'],
						);

						expect($ret)->toBeSame($expected, 'handles REGEXP');

						# not query same case
						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype NOT REGEXP 'jso[a-z]'");
						$expected = array(
							['id' => 1, 'group_id' => 12345, 'mimetype' => 'name1'],
							['id' => 3, 'group_id' => 12345, 'mimetype' => 'name3'],
							['id' => 4, 'group_id' => 6, 'mimetype' => 'name3'],
							['id' => 6, 'group_id' => 6, 'mimetype' => 'name3'],
						);

						expect($ret)->toBeSame($expected, 'handles NOT REGEXP');
					},
				);

				it(
					'handles REGEXP BINARY',
					() ==> {
						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype REGEXP BINARY('jso[A-Z]')");
						expect($ret)->toBeSame([], 'handles REGEXP BINARY with different case');

						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype REGEXP BINARY('jso[a-z]')");
						$expected = array(
							['id' => 2, 'group_id' => 12345, 'mimetype' => 'name2'],
						);
						expect($ret)->toBeSame($expected, 'handles REGEXP BINARY with same case');

						$ret = local_query_run("SELECT * FROM table3 WHERE mimetype NOT REGEXP BINARY('jso[a-z]')");
						$expected = array(
							['id' => 1, 'group_id' => 12345, 'mimetype' => 'name1'],
							['id' => 3, 'group_id' => 12345, 'mimetype' => 'name3'],
							['id' => 4, 'group_id' => 6, 'mimetype' => 'name3'],
							['id' => 6, 'group_id' => 6, 'mimetype' => 'name3'],
						);

						expect($ret)->toBeSame($expected, 'handles NOT REGEXP BINARY');
					},
				);

				it(
					'handles UNION and UNION ALL',
					() ==> {
						$ret = local_query_run(
							"SELECT * from table3 where id = 1 UNION ALL SELECT * from table3 WHERE id = 2 UNION SELECT * from table3 where id = 3 UNION SELECT * from table3 where id = 3",
						);
						$expected = array(
							array('id' => 1, 'group_id' => 12345, 'mimetype' => 'name1'),
							array('id' => 2, 'group_id' => 12345, 'mimetype' => 'name2'),
							# because UNION dedupes, this row is not duplicated even though it's selected twice
							array('id' => 3, 'group_id' => 12345, 'mimetype' => 'name3'),
						);

						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'unescapes text, except specific sequences like \% and \Z',
					() ==> {
						$ret = local_query_run("SELECT '\\\\foo\'sbar\%\Zbaz\/' as testescape");
						expect($ret)->toBeSame(array(array('testescape' => "\\foo'sbar\%\Zbaz/")));
					},
				);

				it(
					'handles double quotes',
					() ==> {
						$ret = local_query_run('SELECT id FROM table3 WHERE mimetype="name1"');
						expect($ret)->toBeSame(array(array('id' => 1)));
					},
				);

				it(
					'handles IS NULL and IS NOT NULL',
					() ==> {
						$ret = local_query_run('SELECT id from table3 LEFT JOIN association_table ON id = table_3_id WHERE table_3_id IS NULL');
						expect($ret)->toBeSame(
							array(
								array('id' => 4),
								array('id' => 6),
							),
						);

						$ret = local_query_run('SELECT id from table3 LEFT JOIN association_table ON id = table_3_id WHERE table_3_id IS NOT NULL');
						expect($ret)->toBeSame(
							array(
								array('id' => 1),
								# this is duplicated because it's shared in two channels, same as MySQL would return
								array('id' => 1),
								array('id' => 2),
								array('id' => 3),
							),
						);
					},
				);

				it(
					'handles IF',
					() ==> {
						$ret = local_query_run("SELECT id, IF(id >= 4, 1, 0) as test FROM table3");
						expect($ret)->toBeSame(
							array(
								array('id' => 1, 'test' => 0),
								array('id' => 2, 'test' => 0),
								array('id' => 3, 'test' => 0),
								array('id' => 4, 'test' => 1),
								array('id' => 6, 'test' => 1),
							),
						);
					},
				);

				it(
					'handles NOT with parentheses',
					() ==> {
						$ret = local_query_run("SELECT id FROM table3 WHERE group_id=12345 AND NOT (mimetype='name1' OR mimetype='name3')");
						expect($ret)->toBeSame(array(array('id' => 2)));
					},
				);

				it(
					'handles SUBSTR',
					() ==> {
						# run the actual MySQL statements in a MySQL database to verify these are the results it gives (it is multi-byte safe)
						$ret = local_query_run("SELECT SUBSTR('foobar', 1, 2) as first, SUBSTR('foobar', 2) as second, SUBSTR('foobar', 3, 2) as third");
						expect($ret)->toBeSame(
							array(
								array('first' => 'fo', 'second' => 'oobar', 'third' => 'ob'),
							),
							'handles ASCII strings',
						);

						$ret = local_query_run(
							"SELECT SUBSTR('よかったですね', 1, 2) as first, SUBSTR('よかったですね', 2) as second, SUBSTR('よかったですね', 3, 2) as third",
						);
						expect($ret)->toBeSame(
							array(
								array('first' => 'よか', 'second' => 'かったですね', 'third' => 'った'),
							),
							'handles multi-byte characters the same way MySQL does',
						);
					},
				);

				it(
					'handles LENGTH',
					() ==> {
						# run the actual MySQL statements in a MySQL database to verify these are the results it gives (it is multi-byte safe)
						$ret = local_query_run("SELECT LENGTH('foobar')");
						expect($ret)->toBeSame(
							array(
								array('LENGTH(\'foobar\')' => 6),
							),
							'counts ASCII strings the same way as MySQL\'s LENGTH()',
						);

						$ret = local_query_run("SELECT LENGTH('よかったですね')");
						expect($ret)->toBeSame(
							array(
								array('LENGTH(\'よかったですね\')' => 21),
							),
							'counts multi-byte characters as MySQL\'s LENGTH()',
						);
					},
				);

				it(
					'handles CHAR_LENGTH',
					() ==> {
						# run the actual MySQL statements in a MySQL database to verify these are the results it gives (it is multi-byte safe)
						$ret = local_query_run("SELECT CHAR_LENGTH('foobar')");
						expect($ret)->toBeSame(
							array(
								array('CHAR_LENGTH(\'foobar\')' => 6),
							),
							'counts ASCII strings the same way as MySQL\'s CHAR_LENGTH()',
						);

						$ret = local_query_run("SELECT CHAR_LENGTH('よかったですね')");
						expect($ret)->toBeSame(
							array(
								array('CHAR_LENGTH(\'よかったですね\')' => 7),
							),
							'counts multi-byte characters as MySQL\'s CHAR_LENGTH()',
						);
					},
				);

				it(
					'handles COALESCE',
					() ==> {
						$ret = local_query_run("SELECT COALESCE(1, 2, 3) as first, COALESCE(NULL, 2) as second, COALESCE(NULL, NULL, NULL) as third");
						expect($ret)->toBeSame(
							array(
								array('first' => 1, 'second' => 2, 'third' => null),
							),
						);
					},
				);

				it(
					'handles GREATEST',
					() ==> {
						$ret = local_query_run("SELECT GREATEST(1, 3, 2) as first, GREATEST(NULL, 2) as second, GREATEST(NULL, NULL, NULL) as third");
						expect($ret)->toBeSame(
							array(
								array('first' => 3, 'second' => 2, 'third' => null),
							),
						);
					},
				);

				it(
					'handles NULLIF',
					() ==> {
						$ret = local_query_run("SELECT NULLIF(1, 2) as first, NULLIF(1, 1) as second");
						expect($ret)->toBeSame(
							array(
								array('first' => 1, 'second' => null),
							),
						);
					},
				);

				it(
					'handles CONCAT_WS',
					() ==> {
						$ret = local_query_run("select concat_ws('-',group_id, table_3_id) as concat_key from association_table where group_id='12345'");
						$expected = array(
							array('concat_key' => "12345-1"),
							array('concat_key' => "12345-1"),
							array('concat_key' => "12345-2"),
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles FIELD',
					() ==> {
						$ret = local_query_run("select id FROM table4 ORDER BY field(id, 1002, 1001, 1004, 1003)");
						$expected = array(
							['id' => 1000],
							['id' => 1002],
							['id' => 1001],
							['id' => 1004],
							['id' => 1003],
						);
						expect($ret)->toBeSame($expected);
					},
				);

				it(
					'handles FOR UPDATE lock hints',
					() ==> {
						$ret = local_query_run("SELECT id, group_id as my_fav_group_id FROM table3 WHERE group_id=6 FOR UPDATE");
						expect($ret)->toBeSame(
							array(
								array('id' => 4, 'my_fav_group_id' => 6),
								array('id' => 6, 'my_fav_group_id' => 6),
							),
						);
					},
				);

				it(
					'handles LOCK IN SHARE MODE hints',
					() ==> {
						$ret = local_query_run("SELECT id, group_id as my_fav_group_id FROM table3 WHERE group_id=6 LOCK IN SHARE MODE");
						expect($ret)->toBeSame(
							array(
								array('id' => 4, 'my_fav_group_id' => 6),
								array('id' => 6, 'my_fav_group_id' => 6),
							),
						);
					},
				);

				it(
					'handles row comparisons with integers',
					() ==> {
						$ret = local_query_run('SELECT (1, 2, 3) > (4, 5, 6)');
						expect($ret)->toBeSame(
							array(
								array('(1, 2, 3) > (4, 5, 6)' => 0),
							),
						);

						$ret = local_query_run('SELECT (1, 2, 3) < (4, 5, 6)');
						expect($ret)->toBeSame(
							array(
								array('(1, 2, 3) < (4, 5, 6)' => 1),
							),
						);

						$ret = local_query_run('SELECT (4, 5, 6) > (1, 2, 3)');
						expect($ret)->toBeSame(
							array(
								array('(4, 5, 6) > (1, 2, 3)' => 1),
							),
						);

						$ret = local_query_run('SELECT (1, 2, 3) > (1, 2, 2)');
						expect($ret)->toBeSame(
							array(
								array('(1, 2, 3) > (1, 2, 2)' => 1),
							),
						);

						$ret = local_query_run('SELECT (1, 2, 3) > (1, 2, 2)');
						expect($ret)->toBeSame(
							array(
								array('(1, 2, 3) > (1, 2, 2)' => 1),
							),
						);

						$ret = local_query_run('SELECT (1, 2, 3) > (1, 1, 4)');
						expect($ret)->toBeSame(
							array(
								array('(1, 2, 3) > (1, 1, 4)' => 1),
							),
						);
					},
				);

				it(
					'handles row comparisons on actual rows',
					() ==> {

						$ret = local_query_run("select * from table3 WHERE (id, group_id) > (3, 1)");

						expect($ret)->toBeSame(
							array(
								array('id' => 3, 'group_id' => 12345, 'mimetype' => 'name3'),
								array('id' => 4, 'group_id' => 6, 'mimetype' => 'name3'),
								array('id' => 6, 'group_id' => 6, 'mimetype' => 'name3'),
							),
						);
					},
				);
			},
		);
		*/
}
