<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class QueryFormatterTest extends HackTest {

	public async function testSingleFormat(): Awaitable<void> {
		$qf = QueryFormatter::formatQuery('SELECT * FROM foo WHERE bar = %s', 'baz');
		expect($qf)->toBeSame('SELECT * FROM foo WHERE bar = "baz"');
	}

	public async function testNullString(): Awaitable<void> {
		$qf = QueryFormatter::formatQuery('SELECT %s FROM foo WHERE bar %=s', null, null);
		expect($qf)->toBeSame('SELECT NULL FROM foo WHERE bar  IS NULL');
	}

	public async function testComplexFormat(): Awaitable<void> {
		$qf = QueryFormatter::formatQuery('SELECT %C, %C FROM %T WHERE %C = %d', 'col1', 'col2', 'mytable', 'col2', 25);
		expect($qf)->toBeSame('SELECT `col1`, `col2` FROM `mytable` WHERE `col2` = 25');
	}

	public async function testInvalidValue(): Awaitable<void> {
		expect(() ==> QueryFormatter::formatQuery("SELECT %d", '1'))->toThrow(
			SQLFakeParseException::class,
			'string value not valid for %d',
		);
	}

	public async function testIdentifierTuples(): Awaitable<void> {
		$qf = QueryFormatter::formatQuery("SELECT %C FROM %T", tuple('tab1', 'col1', 'myalias'), tuple('db1', 'tab1'));
		expect($qf)->toBeSame("SELECT `tab1`.`col1` AS `myalias` FROM `db1`.`tab1`");
	}

	public async function testStringList(): Awaitable<void> {
		$qf = QueryFormatter::formatQuery(
			"SELECT * FROM t1 WHERE name IN (%Ls) OR id IN (%Ld)",
			keyset['foo', 'bar', 'baz'],
			keyset[100, 101, 102],
		);

		expect($qf)->toBeSame('SELECT * FROM t1 WHERE name IN ("foo", "bar", "baz") OR id IN (100, 101, 102)');
	}
}
