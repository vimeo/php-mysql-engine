<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\Vec;

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
  ): Awaitable<AsyncMysqlQueryResult> {
    Logger::log(Verbosity::QUERIES, "SQLFake [verbose]: $query");

    $config = $this->server->config;
    $strict_sql_before = QueryContext::$strictSQLMode;
    if ($config['strict_sql_mode'] ?? false) {
      QueryContext::$strictSQLMode = true;
    }

    $strict_schema_before = QueryContext::$strictSchemaMode;
    if ($config['strict_schema_mode'] ?? false) {
      QueryContext::$strictSchemaMode = true;
    }

    if (($config['inherit_schema_from'] ?? '') !== '') {
      $this->dbname = $config['inherit_schema_from'] ?? '';
    }

    try {
      list($results, $rows_affected) = SQLCommandProcessor::execute($query, $this);
    } catch (\Exception $e) {
      // this makes debugging a failing unit test easier, show the actual query that failed parsing along with the parser error
      QueryContext::$strictSQLMode = $strict_sql_before;
      QueryContext::$strictSchemaMode = $strict_schema_before;
      $msg = $e->getMessage();
      $type = \get_class($e);
      Logger::log(Verbosity::QUIET, "SQL Fake $type: $msg in SQL query: $query");
      throw $e;
    }
    QueryContext::$strictSQLMode = $strict_sql_before;
    QueryContext::$strictSchemaMode = $strict_schema_before;
    Logger::logResult($this->getServer()->name, $results, $rows_affected);
    return new AsyncMysqlQueryResult(vec($results), $rows_affected);
  }

  <<__Override>>
  public async function queryf(
    \HH\FormatString<\HH\SQLFormatter> $query,
    mixed ...$args
  ): Awaitable<AsyncMysqlQueryResult> {
    throw new SQLFakeNotImplementedException('queryf not yet implemented');
  }

  <<__Override>>
  public async function multiQuery(
    Traversable<string> $queries,
    int $_timeout_micros = -1,
    dict<string, string> $_query_attributes = dict[],
  ): Awaitable<Vector<AsyncMysqlQueryResult>> {
    $results = await Vec\map_async($queries, $query ==> $this->query($query));
    return Vector::fromItems($results);
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
