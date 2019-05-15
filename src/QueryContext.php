<?hh // strict

namespace Slack\DBMock;

abstract final class QueryContext {

  /**
   * In strict mode, any query referencing a table not in the shcema
   * will throw an exception
   *
   * This should be turned on if schema is available
   */
  public static bool $strictMode = false;

  /**
   * Representation of database schema
   * String keys are database names, with table names inside that list
   *
   * There is a built-in assumption that you don't have two databases on different
   * servers with the same name but different schemas. We don't include server hostnames here
   * because it's common to have sharded databases with the same names on different hosts
   */
  public static dict<string, dict<string, table_schema>> $schema = dict[];

  public static function getSchema(string $database, string $table): ?table_schema {
    return self::$schema[$database][$table] ?? null;
  }
}
