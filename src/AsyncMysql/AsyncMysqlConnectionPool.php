<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Dict};

/**
 * Mock a connection pool, instantiating fake connections instead of real ones
 */
final class AsyncMysqlConnectionPool extends \AsyncMysqlConnectionPool {

  private int $createdPoolConnections = 0;
  private int $destroyedPoolConnections = 0;
  private int $connectionsRequest = 0;
  private int $poolHits = 0;
  private int $poolMisses = 0;
  private static dict<string, AsyncMysqlConnection> $pool = dict[];

  <<__Override>>
  public async function connect(
    string $host,
    int $port,
    string $dbname,
    string $user,
    string $password,
    int $timeout_micros = -1,
    string $caller = "",
  ): Awaitable<\AsyncMysqlConnection> {
    $this->connectionsRequest++;
    if (C\contains_key(static::$pool, $host)) {
      $this->poolHits++;
      $conn = static::$pool[$host];
      $conn->setDatabase($dbname);
      return $conn;
    }

    $this->poolMisses++;
    $this->createdPoolConnections++;
    return new AsyncMysqlConnection($host, $port, $dbname);
  }

  <<__Override>>
  public function connectWithOpts(
    string $host,
    int $port,
    string $dbname,
    string $user,
    string $password,
    \AsyncMysqlConnectionOptions $conn_opts,
    string $caller = "",
  ): Awaitable<\AsyncMysqlConnection> {
    // currently, options are ignored in DBMock
    return $this->connect($host, $port, $dbname, $user, $password, -1, $caller);
  }

  <<__Override>>
  public function getPoolStats(): darray<string, int> {
    return darray[
      'created_pool_connections' => $this->createdPoolConnections,
      'destroyed_pool_connections' => $this->destroyedPoolConnections,
      'connections_request' => $this->connectionsRequest,
      'pool_hits' => $this->poolHits,
      'pool_misses' => $this->poolMisses,
    ];
  }

  /**
   * Not part of the AsyncMysqlConnectionPool interface, these are for debugging
   */
  public static function getAllServers(): dict<string, Server> {
    return Dict\map(static::$pool, $conn ==> $conn->getServer());
  }

  public static function getServer(string $name): ?Server {
    return C\contains_key(static::$pool, $name) ? static::$pool[$name]->getServer() : null;
  }
}
