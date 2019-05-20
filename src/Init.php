<?hh // strict

/**
 * Top level API meant for invoking by the outside world
 */

namespace Slack\SQLFake;

/**
 *
 * Configure the library with a representation of table schema.
 * This allows SQLFake to provide fully typed rows, validate that columns exist,
 * enforce primary key constraints, check if indexes would be used, and more
 *
 * If schema is not provided, SQLFake will allow all queries and enforce no constraints.
 *
 * If strict mode is provided (recommended), SQLFake will throw an exception on any query referencing tables not in the schema.
 */
function init(dict<string, dict<string, table_schema>> $schema = dict[], bool $strict = false): void {
  QueryContext::$schema = $schema;
  QueryContext::$strictMode = $strict;
}

function snapshot(string $name): void {
  Server::snapshot($name);
}

function restore(string $name): void {
  Server::restore($name);
}
