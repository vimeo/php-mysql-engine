<?hh // strict

namespace Slack\DBMock;

use type Facebook\HackTest\HackTest;

final class UpdateQueryTest extends HackTest {
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
}
