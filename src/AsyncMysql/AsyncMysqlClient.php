<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\Vec;

/* HHAST_IGNORE_ALL[UnusedParameter] */

/* HH_IGNORE_ERROR[2049] */
<<__MockClass>>
final class AsyncMysqlClient extends \AsyncMysqlClient {

	/* HH_IGNORE_ERROR[3012] I don't want parent::construct to be called */
	public function __construct() {}

	<<__Override>>
	public static function setPoolsConnectionLimit(int $_limit): void {}

	<<__Override>>
	public static async function connect(
		string $host,
		int $port,
		string $dbname,
		string $user,
		string $password,
		int $timeout_micros = -1,
		?\MySSLContextProvider $ssl_provider = null,
	): Awaitable<\AsyncMysqlConnection> {
		return new AsyncMysqlConnection($host, $port, $dbname);
	}

	<<__Override>>
	public static async function connectWithOpts(
		string $host,
		int $port,
		string $dbname,
		string $user,
		string $password,
		\AsyncMysqlConnectionOptions $conn_opts,
	): Awaitable<\AsyncMysqlConnection> {
		return new AsyncMysqlConnection($host, $port, $dbname);
	}

	<<__Override>>
	public static async function connectAndQuery(
		Traversable<string> $queries,
		string $host,
		int $port,
		string $dbname,
		string $user,
		string $password,
		\AsyncMysqlConnectionOptions $conn_opts,
		dict<string, string> $query_attributes = dict[],
	): Awaitable<(\AsyncMysqlConnectResult, Vector<\AsyncMysqlQueryResult>)> {
		$conn = new AsyncMysqlConnection($host, $port, $dbname);
		$results = await Vec\map_async($queries, $query ==> $conn->query($query));
		$results = Vector::fromItems($results);
		return tuple($conn->connectResult(), $results);
	}

	<<__Override>>
	public static function adoptConnection(\AsyncMysqlConnection $connection): void {}
}
