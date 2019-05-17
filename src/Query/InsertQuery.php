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

        $unique_key_violation = DataIntegrity::checkUniqueConstraints($table, $row, $schema);
        if ($unique_key_violation is nonnull) {
          list($msg, $row_id) = $unique_key_violation;
          // is this an "INSERT ... ON DUPLICATE KEY UPDATE?"
          // if so, this is where we apply the updates
          if (!C\is_empty($this->updateExpressions)) {
            $existing_row = $table[$row_id];
            list($affected, $table) = $this->applySet(
              $conn,
              $database,
              $table_name,
              dict[$row_id => $existing_row],
              $table,
              $this->updateExpressions,
              $schema,
              $row,
            );
            $rows_affected += $affected;
            continue;
          } else {
            // otherwise throw
            throw new DBMockUniqueKeyViolation($msg);
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
