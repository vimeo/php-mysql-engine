<?hh // strict

namespace Slack\DBMock;

/* HH_IGNORE_ERROR[2049] */
<<__MockClass>>
final class AsyncMysqlConnection extends \AsyncMysqlConnection {

	private bool $open = true;
	private bool $reusable = true;
	private AsyncMysqlConnectResult $result;

	/**
	 * Not part of the built-in AsyncMysqlConnection
	 */
	private Server $server;

	public function getServer(): Server {
		return $this->server;
	}

	public function getDatabase(): string {
		return $this->dbname;
	}

	public function setDatabase(string $dbname): void {
		$this->dbname = $dbname;
	}

	/* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
	public function __construct(
		private string $host,
		private int $port,
		private string $dbname,
	) {
		// in the fake database world, each connection gets a different logical server (enforcing no data sharing between hosts)
		$this->server = new Server($host);
		$this->result = new AsyncMysqlConnectResult(false);
	}

	public async function query(
		string $query,
		int $timeout_micros = -1,
		dict<string, string> $query_attributes = dict[],
	): Awaitable<\AsyncMysqlQueryResult> {
		$cmd = new SQLCommandProcessor();
		list($results, $rows_affected) = $cmd->execute($query, $this);
		return new AsyncMysqlQueryResult($results, $rows_affected);
	}

	public async function queryf(
		\HH\FormatString<\HH\SQLFormatter> $query,
		mixed ...$args
	): Awaitable<\AsyncMysqlQueryResult> {
		throw new DBMockNotImplementedException('queryf not yet implemented');
	}

	public function multiQuery(
		Traversable<string> $query,
		int $timeout_micros = -1,
		dict<string, string> $query_attributes = dict[],
	): mixed {
		throw
			new DBMockNotImplementedException('multiQuery not yet implemented');
	}

	public function escapeString(string $data): string {
		// TODO not implemented
		return $data;
	}

	public function close(): void {
		$this->open = false;
	}

	public function releaseConnection(): void {}

	public function isValid(): bool {
		return $this->open;
	}

	public function serverInfo(): mixed {
		return null;
	}

	public function warningCount(): int {
		return 0;
	}

	public function host(): string {
		return $this->host;
	}
	public function port(): int {
		return $this->port;
	}
	public function setReusable(bool $reusable): void {
		$this->reusable = $reusable;
	}
	public function isReusable(): bool {
		return $this->reusable;
	}

	public function lastActivityTime(): mixed {
		return null;
	}
	public function connectResult(): \AsyncMysqlConnectResult {
		return $this->result;
	}
}
