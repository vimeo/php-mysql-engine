<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Dict, Keyset, Str};

/**
 * An executable Query plan
 *
 * Clause processors used by multiple query types are implemented here
 * Any clause used by only one query type is processed in that subclass
 */
abstract class Query {

  public ?Expression $whereClause = null;
  public ?order_by_clause $orderBy = null;
  public ?limit_clause $limitClause = null;

  /**
   * The initial query that was executed, no longer needed after parsing but retained for
   * debugging and logging
   */
  public string $sql;

  protected function applyWhere(AsyncMysqlConnection $conn, dataset $data): dataset {
    $where = $this->whereClause;
    if ($where === null) {
      // no where clause? cool! just return the given data
      return $data;
    }

    return Dict\filter($data, $row ==> (bool)$where->evaluate($row, $conn));
  }

  /**
   * Apply the ORDER BY clause to sort the rows
   */
  protected function applyOrderBy(AsyncMysqlConnection $conn, dataset $data): dataset {
    $order_by = $this->orderBy;
    if ($order_by === null) {
      return $data;
    }

    // allow all column expressions to fall through to the full row
    foreach ($order_by as $k => $rule) {
      $expr = $rule['expression'];
      if ($expr is ColumnExpression) {
        $expr->allowFallthrough();
      }
    }

    // Work around default sorting behavior to provide a usort that looks like MySQL, where equal values are ordered deterministically
    $index = 0;

    // sort function applies all ORDER BY criteria to compare two rows
    $sort_fun = (row $a, row $b): int ==> {
      foreach ($order_by as $rule) {

        $value_a = $rule['expression']->evaluate($a, $conn);
        $value_b = $rule['expression']->evaluate($b, $conn);

        if ($value_a != $value_b) {
          if ($value_a is num && $value_b is num) {
            return (
              ((float)$value_a < (float)$value_b ? 1 : 0) ^ (($rule['direction'] === SortDirection::DESC) ? 1 : 0)
            )
              ? -1
              : 1;
          } else {
            return (
              ((string)$value_a < (string)$value_b ? 1 : 0) ^ (($rule['direction'] === SortDirection::DESC) ? 1 : 0)
            )
              ? -1
              : 1;
          }

        }
      }
      return 0;
    };

    // record the keys in a dict for usort
    $data_temp = dict[];
    foreach ($data as $i => $item) {
      $data_temp[$i] = tuple($index++, $item);
    }

    $data_temp = Dict\sort($data_temp, ((int, dict<string, mixed>) $a, (int, dict<string, mixed>) $b): int ==> {
      $result = $sort_fun($a[1], $b[1]);

      return $result === 0 ? $b[0] - $a[0] : $result;
    });

    // re-key the input dataset
    $data = dict($data);
    foreach ($data_temp as $index => $item) {
      $data[$index] = $item[1];
    }

    return $data;
  }

  protected function applyLimit(dataset $data): dataset {
    $limit = $this->limitClause;
    if ($limit === null) {
      return $data;
    }
    // this is like Vec\slice(), but there's no Dict\slice
    // we want to retain keys as row numbers for update statements, so we need a dict
    return dict(\array_slice($data, $limit['offset'], $limit['rowcount']));
  }

  /**
   * Parses a table name that may contain a . to reference another database
   * Returns the fully qualified database name and table name as a tuple
   * If there is no ".", the database name will be the connection's current database
   */
  public static function parseTableName(AsyncMysqlConnection $conn, string $table): (string, string) {
    // referencing a table from another database on the same server?
    if (Str\contains($table, '.')) {
      $parts = Str\split($table, '.');
      if (C\count($parts) !== 2) {
        throw new DBMockRuntimeException("Table name $table has too many parts");
      }
      list($database, $table_name) = $parts;
      return tuple($database, $table_name);
    } else {
      // otherwise use connection context's database
      $database = $conn->getDatabase();
      return tuple($database, $table);
    }
  }


  /**
   * Apply the "SET" clause of an UPDATE, or "ON DUPLICATE KEY UPDATE"
   */
  protected function applySet(
    AsyncMysqlConnection $conn,
    string $database,
    string $table_name,
    dataset $filtered_rows,
    dataset $original_table,
    vec<BinaryOperatorExpression> $set_clause,
    ?table_schema $table_schema,
  ): int {

    $original_table as vec<_>;

    $valid_fields = null;
    if ($table_schema !== null) {
      $valid_fields = Keyset\map($table_schema['fields'], $field ==> $field['name']);
    }

    $set_clauses = vec[];
    foreach ($set_clause as $expression) {
      // the parser already asserts this at parse time
      $left = $expression->left as ColumnExpression;
      $right = $expression->right as nonnull;
      $column = $left->name;

      # If we know the valid fields for this table, only allow setting those
      if ($valid_fields !== null) {
        if (!C\contains($valid_fields, $column)) {
          throw new DBMockRuntimeException("Invalid update column {$column}");
        }
      }

      $set_clauses[] = shape('column' => $column, 'expression' => $right);
    }

    $update_count = 0;

    foreach ($filtered_rows as $row_id => $row) {
      $changes_found = false;
      foreach ($set_clauses as $clause) {
        $existing_value = $row[$clause['column']];
        $new_value = $clause['expression']->evaluate($row, $conn);

        if ($new_value !== $existing_value) {
          $row[$clause['column']] = $new_value;
          $changes_found = true;
        }
      }

      if ($changes_found) {
        $original_table[$row_id] = $row;
        $update_count++;
      }
    }

    // write it back to the database
    $conn->getServer()->saveTable($database, $table_name, $original_table);
    return $update_count;
  }

}
