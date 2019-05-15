<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

final class InsertQuery extends Query {

  public function __construct(public string $table, public string $sql, public bool $ignoreDupes) {}

  public vec<BinaryOperatorExpression> $updateExpressions = vec[];
  public vec<string> $insertColumns = vec[];
  public vec<vec<Expression>> $values = vec[];

  /**
   * Insert rows, with validation
   * Returns number of rows affected
   */
  public function execute(AsyncMysqlConnection $conn): int {
    list($database, $table_name) = Query::parseTableName($conn, $this->table);
    $table = $conn->getServer()->getTable($database, $table_name) ?? vec[];

    Metrics::trackQuery(QueryType::INSERT, $conn->getServer()->name, $table_name, $this->sql);

    $schema = QueryContext::getSchema($database, $table_name);
    if ($schema === null && QueryContext::$strictMode) {
      throw new DBMockRuntimeException("Table $table_name not found in schema and strict mode is enabled");
    }

    $rows_affected = 0;
    foreach ($this->values as $value_list) {
      $row = dict[];
      foreach ($this->insertColumns as $key => $col) {
        $row[$col] = $value_list[$key]->evaluate(dict[], $conn);
      }

      // can't enforce uniqueness or defaults if there is no schema available
      if ($schema === null) {
        $table[] = $row;
        $rows_affected++;
        continue;
      }

      // ensure all fields are present with appropriate types and default values
      // throw for nonexistent fields
      $row = DataIntegrity::coerceToSchema($table, $row, $schema);

      // check for unique key violations unless INSERT IGNORE was specified
      if (!$this->ignoreDupes) {
        try {
          // TODO change this, instead of throwing it should return whether there is a violation and the integer id of the row that violated
          // so that we can use for that for updates
          DataIntegrity::checkUniqueConstraints($table, $row, $schema);
        } catch (DBMockUniqueKeyViolation $e) {
          // is this an "INSERT ... ON DUPLICATE KEY UPDATE?"
          // if so, this is where we apply the updates
          if (!C\is_empty($this->updateExpressions)) {
            // TODO apply update here

            // $update_expression !== null && $database !== null) {
            // manual update expression is used when the UPDATE clause contains more than just scalars in an INSERT ... ON DUPLICATE KEY UPDATE. in this case, we need to apply that expression
            $old_row['db_mock_row_id'] = $row_num;
            $table_schema = db_tables_get_schema(_db_mock_get_cluster_type($cluster), $table_name);
            $rows_affected = db_mock_query_apply_set(
              $table_name,
              $database,
              Vector {new Map($old_row)},
              $update_expression,
              $table_schema,
            );
          } else {
            // otherwise re-throw
            throw $e;
          }
        }
      }
      $table[] = $row;
      $rows_affected++;
    }

    // write it back to the database
    $conn->getServer()->saveTable($database, $table_name, $table);
    return $rows_affected;
  }
}
