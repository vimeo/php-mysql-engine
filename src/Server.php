<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

final class Server {

  public function __construct(public string $name, public ?server_config $config = null) {}

  private static dict<string, Server> $instances = dict[];
  private static keyset<string> $snapshot_names = keyset[];

  public static function getAll(): dict<string, this> {
    return static::$instances;
  }

  public static function get(string $name): ?this {
    return static::$instances[$name] ?? null;
  }

  public function setConfig(server_config $config): void {
    $this->config = $config;
  }

  public static function getOrCreate(string $name): this {
    $server = static::$instances[$name] ?? null;
    if ($server === null) {
      $server = new static($name);
      static::$instances[$name] = $server;
    }
    return $server;
  }

  public static function reset(): void {
    foreach (static::getAll() as $server) {
      $server->doReset();
    }
  }

  public static function snapshot(string $name): void {
    foreach (static::getAll() as $server) {
      $server->doSnapshot($name);
    }
    static::$snapshot_names[] = $name;
  }

  public static function restore(string $name): void {
    if (!C\contains_key(static::$snapshot_names, $name)) {
      throw new SQLFakeRuntimeException("Snapshot $name not found, unable to restore");
    }
    foreach (static::getAll() as $server) {
      $server->doRestore($name);
    }
  }

  protected function doSnapshot(string $name): void {
    $this->snapshots[$name] = $this->databases;
  }

  protected function doRestore(string $name): void {
    $this->databases = $this->snapshots[$name] ?? dict[];
  }

  protected function doReset(): void {
    $this->databases = dict[];
  }

  /**
   * The main storage mechanism
   * dict of strings (database schema names)
   * -> dict of string table names to tables
   * -> vec of rows
   * -> dict of string column names to columns
   *
   * While a structure based on objects all the way down the stack may be more powerful and readable,
   * This structure uses value types intentionally, to enable a relatively efficient reset/snapshot logic
   * which is often used frequently between test cases
   */
  public dict<string, dict<string, vec<dict<string, mixed>>>> $databases = dict[];
  private dict<string, dict<string, dict<string, vec<dict<string, mixed>>>>> $snapshots = dict[];

  /**
   * Retrieve a table from the specified database, if it exists, by value
   */
  public function getTable(string $dbname, string $name): ?vec<dict<string, mixed>> {
    return $this->databases[$dbname][$name] ?? null;
  }

  /**
   * Save a table's rows back to the database
   * note, because insert and update operations already grab the full table for checking constraints,
   * we don't bother providing an insert or update helper here.
   */
  public function saveTable(string $dbname, string $name, vec<dict<string, mixed>> $rows): void {
    // create table if not exists
    if (!C\contains_key($this->databases, $dbname)) {
      $this->databases[$dbname] = dict[];
    }

    // save rows
    $this->databases[$dbname][$name] = $rows;
  }
}
