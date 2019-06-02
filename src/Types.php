<?hh // strict

namespace Slack\SQLFake;

//
// Data Storage Types
//

// a single DB row
type row = dict<string, mixed>;
// vec of rows can be a stored table, a query result set, or an intermediate state for either of those
type dataset = KeyedContainer<int, row>;
// a database is a collection of named tables
type database = dict<string, dataset>;

//
// Parser
//

type token = shape(
  'type' => TokenType,
  'value' => string,
  // the raw token including capitalization, quoting, and whitespace. used for generating SELECT column names for expressions
  'raw' => string,
);

enum TokenType: string {
  NUMERIC_CONSTANT = "Number";
  STRING_CONSTANT = "String";
  CLAUSE = "Clause";
  OPERATOR = "Operator";
  RESERVED = "Reserved";
  PAREN = "Paren";
  SEPARATOR = "Separator";
  SQLFUNCTION = "Function";
  IDENTIFIER = "Identifier";
  NULL_CONSTANT = "Null";
}

enum JoinType: string {
  JOIN = 'JOIN';
  LEFT = 'LEFT';
  RIGHT = 'RIGHT';
  CROSS = 'CROSS';
  STRAIGHT = 'STRAIGHT_JOIN';
  NATURAL = 'NATURAL';
}

enum Verbosity: int as int {
  // Default, print nothing
  QUIET = 1;
  // Print every query as it executes
  QUERIES = 2;
  // Print every query and its results
  RESULTS = 3;
}

enum JoinOperator: string {
  ON = "ON";
  USING = "USING";
}

enum SortDirection: string {
  ASC = 'ASC';
  DESC = 'DESC';
}

enum MultiOperand: string {
  UNION = 'UNION';
  UNION_ALL = 'UNION_ALL';
  EXCEPT = 'EXCEPT';
  INTERSECT = 'INTERSECT';
}

type token_list = vec<token>;

type from_table = shape(
  'name' => string,
  ?'subquery' => SubqueryExpression,
  'join_type' => JoinType,
  ?'join_operator' => JoinOperator,
  ?'alias' => string,
  ?'join_expression' => ?Expression,
);

type limit_clause = shape(
  'rowcount' => int,
  'offset' => int,
);

type order_by_clause = vec<shape('expression' => Expression, 'direction' => SortDirection)>;

/**
 * A simple representation of a table schema, used to make the application smarter.
 * This allows SQL Fake to provide fully typed rows, validate that columns exist,
 * enforce primary key constraints, check if indexes would be used, and more
 */
type table_schema = shape(

  /**
   * Table name as it exists in the database
   */
  "name" => string,
  "fields" => Container<
    shape(
      'name' => string,
      'type' => DataType,
      'length' => int,
      'null' => bool,
      'hack_type' => string,
      ?'default' => string,
    ),
  >,
  "indexes" => Container<
    shape(
      'name' => string,
      'type' => string,
      'fields' => Container<string>,
    ),
  >,
);

enum DataType: string {
  TINYINT = 'TINYINT';
  SMALLINT = 'SMALLINT';
  INT = 'INT';
  BIT = 'BIT';
  BIGINT = 'BIGINT';
  FLOAT = 'FLOAT';
  DOUBLE = 'DOUBLE';
  BINARY = 'BINARY';
  CHAR = 'CHAR';
  ENUM = 'ENUM';
  SET = 'SET';
  TINYBLOB = 'TINYBLOB';
  BLOB = 'BLOB';
  MEDIUMBLOB = 'MEDIUMBLOB';
  LONGBLOB = 'LONGBLOB';
  TEXT = 'TEXT';
  TINYTEXT = 'TINYTEXT';
  MEDIUMTEXT = 'MEDIUMTEXT';
  LONGTEXT = 'LONGTEXT';
  VARCHAR = 'VARCHAR';
  VARBINARY = 'VARBINARY';
  JSON = 'JSON';
  DATE = 'DATE';
  DATETIME = 'DATETIME';
  TIME = 'TIME';
  YEAR = 'YEAR';
  TIMESTAMP = 'TIMESTAMP';
  DECIMAL = 'DECIMAL';
  NUMERIC = 'NUMERIC';
}


type server_config = shape(
  // i.e. 5.6, 5.7
  'mysql_version' => string,
  ?'is_vitess' => bool,
  'strict_sql_mode' => bool,
  'strict_schema_mode' => bool,
  // name of a database in table configuration to copy schema from
  ?'inherit_schema_from' => string,
);
