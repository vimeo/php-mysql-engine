<?hh // strict

namespace Slack\DBMock;

/* HHAST_IGNORE_ALL[UnusedParameter] */

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
  public function __construct(private string $host, private int $port, private string $dbname) {
    $this->server = Server::getOrCreate($host);
    $this->result = new AsyncMysqlConnectResult(false);
  }

  <<__Override>>
  public async function query(
    string $query,
    int $timeout_micros = -1,
    dict<string, string> $query_attributes = dict[],
  ): Awaitable<\AsyncMysqlQueryResult> {
    Logger::log(Verbosity::QUERIES, "DBMock [verbose]: $query");
    list($results, $rows_affected) = SQLCommandProcessor::execute($query, $this);
    Logger::logResult($this->getServer()->name, $results, $rows_affected);
    return new AsyncMysqlQueryResult(vec($results), $rows_affected);
  }

  <<__Override>>
  public async function queryf(
    \HH\FormatString<\HH\SQLFormatter> $query,
    mixed ...$args
  ): Awaitable<\AsyncMysqlQueryResult> {
    throw new DBMockNotImplementedException('queryf not yet implemented');
  }

  <<__Override>>
  public function multiQuery(
    Traversable<string> $query,
    int $timeout_micros = -1,
    dict<string, string> $query_attributes = dict[],
  ): mixed {
    throw new DBMockNotImplementedException('multiQuery not yet implemented');
  }

  <<__Override>>
  public function escapeString(string $data): string {
    // TODO not implemented
    return $data;
  }

  <<__Override>>
  public function close(): void {
    $this->open = false;
  }

  <<__Override>>
  public function releaseConnection(): void {}

  <<__Override>>
  public function isValid(): bool {
    return $this->open;
  }

  <<__Override>>
  public function serverInfo(): mixed {
    return null;
  }

  <<__Override>>
  public function warningCount(): int {
    return 0;
  }

  <<__Override>>
  public function host(): string {
    return $this->host;
  }

  <<__Override>>
  public function port(): int {
    return $this->port;
  }

  <<__Override>>
  public function setReusable(bool $reusable): void {
    $this->reusable = $reusable;
  }

  <<__Override>>
  public function isReusable(): bool {
    return $this->reusable;
  }

  <<__Override>>
  public function lastActivityTime(): mixed {
    return null;
  }

  <<__Override>>
  public function connectResult(): \AsyncMysqlConnectResult {
    return $this->result;
  }
}
