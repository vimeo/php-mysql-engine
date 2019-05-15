<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

final class Server {

  public function __construct(public string $name) {}

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
