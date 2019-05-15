<?hh // strict

namespace Slack\DBMock;

/**
 * Pass in a representation of table schema.
 * This allows db mock to provide fully typed rows, validate that columns exist,
 * enforce primary key constraints, check if indexes would be used, and more
 *
 * If schema is not provided, DB mock will allow all queries and enforce no constraints.
 *
 * If strict mode is provided (recommended), DB mock will throw an exception on any query referencing tables not in the schema.
 */
function init(dict<string, dict<string, table_schema>> $schema = dict[], bool $strict = false): void {
  QueryContext::$schema = $schema;
  QueryContext::$strictMode = $strict;
}
