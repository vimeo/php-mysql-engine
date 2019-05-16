<?hh // strict

namespace Slack\DBMock;
use function Facebook\FBExpect\expect;
use type Facebook\HackTest\{DataProvider, HackTest};
use namespace HH\Lib\Str;

final class InsertQueryTest extends HackTest {

	public static async function beforeFirstTestAsync(): Awaitable<void> {
		init(TEST_SCHEMA, true);
		QueryContext::$verbosity = Verbosity::RESULTS;
	}

	public async function testExample(): Awaitable<void> {
		$pool = new AsyncMysqlConnectionPool(darray[]);
		$conn = await $pool->connect("example", 1, 'db1', '', '');
		$results = await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'test')");
		$results = await $conn->query("INSERT INTO table1 (id, name) VALUES (2, 'testing')");
		$results = await $conn->query("INSERT INTO table1 (id, name) VALUES (3, 'testyada')");
		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter inserts";
		#\var_dump($results);
		$results = await $conn->query("UPDATE table1 SET name='updated' WHERE id=2");
		#\var_dump($results);
		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter updates";
		#\var_dump($results);
		Server::snapshot('test');
		$results = await $conn->query("DELETE FROM table1 WHERE id=2");
		#\var_dump($results);
		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter deletes";
		#\var_dump($results);
		Server::restore('test');
		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter restore";
		#\var_dump($results);

		$results =
			await $conn->query("INSERT INTO table1 (id, name) VALUES (1, 'dupe') ON DUPLICATE KEY UPDATE name='dupe'");
		#\var_dump($results);

		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter dupe inserts";
		#\var_dump($results);

		$results = await $conn->query(
			"INSERT INTO table1 (id, name) VALUES (1, 'duplicate') ON DUPLICATE KEY UPDATE name=VALUES(name)",
		);
		#\var_dump($results);

		$results = await $conn->query("SELECT * FROM table1");
		#echo "\nafter dupe inserts 2";
		#\var_dump($results);
	}
	/*
		public function provideDirtyData(): vec<mixed> {
			$elements = vec['the', 'quicky', 'brown', 'fox', 1];
			return vec[
				tuple($elements),
				tuple(new Vector($elements)),
				tuple(new Set($elements)),
				tuple(new Map($elements)),
				tuple(vec($elements)),
				tuple(keyset($elements)),
				tuple(dict($elements)),
				tuple(HackLibTestTraversables::getIterator($elements)),
			];
		}

		<<DataProvider('provideDirtyData')>>
		public function testDirtyData(Traversable<string> $traversable): void {
			expect(Str\join($traversable, '-'))->toBeSame('the-quick-brown-fox-1');
		}

		public function provideNoData(): vec<mixed> {
			return vec[];
		}

		<<DataProvider('provideNoData')>>
		public function testNoData(int $a): void {
			expect($a)->toBeSame(1);
		}

		<<DataProvider('provideNoData')>>
		public function testNoDataDup(int $a): void {
			expect($a)->toBeSame(1);
		}

		public function provideError(): vec<mixed> {
			invariant(
				0 === 1,
				"This test depends on a provider that throws an error.",
			);
			return vec[
				tuple(1, 2),
				tuple(2, 1),
			];
		}

		<<DataProvider('provideError')>>
		public function testProviderError(int $_a, int $_b): void {}

		<<DataProvider('provideError')>>
		public function testProviderErrorDup(int $_a, int $_b): void {}
		*/
}
