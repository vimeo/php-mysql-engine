<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Dict, Keyset, Str, Vec};

final class SelectQuery extends Query {

  public vec<Expression> $selectExpressions = vec[];
  public ?FromClause $fromClause = null;
  public ?vec<Expression> $groupBy = null;
  public ?Expression $havingClause = null;
  public vec<shape('type' => MultiOperand, 'query' => SelectQuery)> $multiQueries = vec[];

  public keyset<string> $options = keyset[];
  // this tracks whether we found a comma in between expressions
  public bool $needsSeparator = false;
  public bool $mostRecentHasAlias = false;

  public function __construct(public string $sql) {}

  public function addSelectExpression(Expression $expr): void {
    if ($this->needsSeparator) {
      throw new DBMockParseException("Unexpected expression!");
    }
    $this->selectExpressions[] = $expr;
    $this->needsSeparator = true;
    $this->mostRecentHasAlias = false;
  }

  public function addOption(string $option): void {
    $this->options[] = $option;
  }

  public function aliasRecentExpression(string $name): void {
    $k = C\last_key($this->selectExpressions);
    if ($k === null || $this->mostRecentHasAlias) {
      throw new DBMockParseException("Unexpected AS");
    }
    $this->selectExpressions[$k]->name = $name;
    $this->mostRecentHasAlias = true;
  }

  public function addMultiQuery(MultiOperand $type, SelectQuery $query): void {
    $this->multiQueries[] = shape('type' => $type, 'query' => $query);
  }

  /**
   * Run the query
   * The 2nd parameter is for supporting correlated subqueries, not currently supported
   */
  public function execute(AsyncMysqlConnection $conn, ?row $_ = null): dataset {

    return
      // FROM clause handling - builds a data set including extracting rows from tables, applying joins
      $this->applyFrom($conn)
      // WHERE caluse - filter out any rows that don't match it
      |> $this->applyWhere($conn, $$)
      // GROUP BY clause - may group the rows if necessary. all clauses after this need to know how to handled both grouped and ungrouped inputs
      |> $this->applyGroupBy($conn, $$)
      // HAVING clause, filter out any rows not matching it
      |> $this->applyHaving($conn, $$)
      // SELECT clause. this is where we actually run the expressions in the SELECT
      |> $this->applySelect($conn, $$)
      // ORDER BY. this runs after select because it could use expressions from the select
      |> $this->applyOrderBy($conn, $$)
      // LIMIT clause
      |> $this->applyLimit($$)
      // filter out any data that we needed for the ORDER BY that is not supposed to be returned
      |> $this->removeOrderByExtras($conn, $$)
      // this recurses in case there are any UNION, EXCEPT, INTERSECT keywords
      |> $this->processMultiQuery($conn, $$);
  }


  /**
   * The FROM clause of the query gets processed first, retrieving data from tables, executing subqueries, and handling joins
   * This is also where we build up the $columns list which is commonly used throughout the entire library to map column references to indexes in this dataset
   */
  protected function applyFrom(AsyncMysqlConnection $conn): dataset {

    $from = $this->fromClause;
    if ($from === null) {
      // we put one empty row when there is no FROM so that queries like "SELECT 1" will return a row
      return vec[dict[]];
    }

    return $from->process($conn, $this->sql);
  }


  /**
   * Apply the GROUP BY clause to group rows by a set of expressions.
   * This may also group the rows if the select list contains an aggregate function, which requires an implicit grouping
   */
  protected function applyGroupBy(AsyncMysqlConnection $conn, dataset $data): dataset {
    $group_by = $this->groupBy;
    $select_expressions = $this->selectExpressions;
    if ($group_by !== null) {

      $grouped_data = dict[];
      foreach ($data as $row) {
        $hashes = '';
        foreach ($group_by as $expr) {
          $hashes .= \sha1($expr->evaluate($row, $conn));
        }
        $hash = \sha1($hashes);
        if (!C\contains_key($hash, $grouped_data))
          $grouped_data[$hash] = dict[];
        $count = C\count($grouped_data[$hash]);
        $grouped_data[$hash][(string)$count] = $row;
      }

      $data = vec[$grouped_data];
    } else {
      $found_aggregate = false;
      foreach ($select_expressions as $expr) {
        if ($expr is FunctionExpression && $expr->isAggregate()) {
          $found_aggregate = true;
          break;
        }
      }

      // if we have an aggregate function in the select clause but no group by, do an implicit group that puts all rows in one grouping
      // this makes things like "SELECT COUNT(*) FROM mytable" work

      // TODO this might be wrong
      if ($found_aggregate) {
        $data = vec[dict['1' => $data]];
      }
    }

    return $data;
  }

  /**
   * Apply the HAVING clause to every (maybe grouped) row in the data set. Only return truthy results.
   */
  protected function applyHaving(AsyncMysqlConnection $conn, dataset $data): dataset {
    if ($this->havingClause is nonnull) {
      return Vec\filter($data, $row ==> (bool)$this->havingClause->evaluate($row, $conn));
    }

    return $data;
  }


  /**
   * Generate the result set containing SELECT expressions
   */
  protected function applySelect(AsyncMysqlConnection $conn, dataset $data): dataset {

    // The ORDER BY portion of queries run after this SELECT code.
    // However, it is possible to ORDER BY a field that we do not intend to SELECT by,
    // or put another way: that we do not intend to return from the query.
    //
    // But we have to include those here anyway so we can perform the ORDER BY
    // and then throw them away.

    $order_by_expressions = $this->orderBy ?? vec[];

    $out = vec[];

    // ok now you got that filter, let's do the formatting
    foreach ($data as $row) {
      $formatted_row = dict[];

      foreach ($this->selectExpressions as $expr) {
        if ($expr is ColumnExpression && $expr->name === '*') {
          // if it's a GROUP BY, take the first row from each grouping.
          // SELECT * with a GROUP BY effectively picks the first row from each group
          $first_value = C\first($row);
          if ($first_value is dict<_, _>) {
            $row = $first_value;
          }
          foreach ($row as $col => $val) {
            $parts = Str\split((string)$col, ".");
            if ($expr->tableName() is nonnull) {
              list($col_table_name, $col_name) = $parts;
              if ($col_table_name == $expr->tableName()) {
                if (!C\contains_key($formatted_row, $col)) {
                  $formatted_row[$col_name] = $val;
                }
              }
            } else {
              $col = C\last($parts);
              if (!C\contains_key($formatted_row, $col)) {
                $formatted_row[$col] = $val;
              }
            }

          }
          continue;
        }

        list($name, $val) = $expr->evaluateWithName($row, $conn);

        // subquery: unroll the expression to get the value out
        if ($expr is SubqueryExpression) {
          invariant($val is KeyedContainer<_, _>, 'subquery results must be KeyedContainer');
          if (C\count($val) > 1) {
            throw new DBMockRuntimeException("Subquery returned more than one row");
          }
          if (C\count($val) === 0) {
            $val = null;
          } else {
            foreach ($val as $r) {
              $r as KeyedContainer<_, _>;
              if (C\count($r) !== 1) {
                throw new DBMockRuntimeException("Subquery result should contain 1 column");
              }
              $val = C\onlyx($r);
            }
          }
        }
        $formatted_row[$name] = $val;
      }

      // Adding any fields needed by the ORDER BY not already returned by the SELECT
      foreach ($order_by_expressions as $order_by) {
        $row as dict<_, _>;
        list($name, $val) = $order_by['expression']->evaluateWithName(/* HH_FIXME[4110] generics */ $row, $conn);
        if (!C\contains_key($formatted_row, $name)) {
          $formatted_row[$name] = $val;
        }
      }

      $out[] = $formatted_row;
    }

    if (C\contains_key($this->options, 'DISTINCT')) {

      return /* HH_FIXME[4110] generics */ Vec\unique($out);
    }

    return /* HH_FIXME[4110] generics */ $out;
  }


  /**
   * Remove fields that we do not SELECT by, but we do ORDER BY
   */
  protected function removeOrderByExtras(AsyncMysqlConnection $_conn, dataset $data): dataset {

    $order_by = $this->orderBy;
    if ($order_by === null || C\count($data) === 0) {
      return $data;
    }

    $order_by_names = keyset[];
    $select_field_names = keyset[];

    foreach ($this->selectExpressions as $expr) {
      $name = $expr->name;
      // if we are selecting everything we know the field is included
      if ($name == "*") {
        return $data;
      }
      if ($name !== null) {
        $select_field_names[] = $name;
      }
    }

    foreach ($order_by as $o) {
      $name = $o['expression']->name;
      if ($name !== null) {
        $order_by_names[] = $name;
      }
    }

    $remove_fields = Keyset\diff($order_by_names, $select_field_names);
    if (C\is_empty($remove_fields)) {
      return $data;
    }

    # remove the fields we don't want from each row
    return Vec\map($data, $row ==> Dict\filter_keys($row, $field ==> !C\contains_key($remove_fields, $field)));
  }


  /**
   * Process a query that contains multiple queries such as with UNION, INTERSECT, EXCEPT, UNION ALL
   */
  protected function processMultiQuery(AsyncMysqlConnection $conn, dataset $data): dataset {

    // function used to stringify rows for comparison
    $row_encoder = (row $row): string ==> Str\join(Vec\map($row, $col ==> (string)$col), '-');

    foreach ($this->multiQueries as $sub) {

      // invoke the subquery
      $subquery_results = $sub['query']->execute($conn);

      // now put the results together based on the keyword
      switch ($sub['type']) {
        case MultiOperand::UNION:
          // contact the results, then get unique rows by converting all fields to string and comparing a joined-up representation
          $data = Vec\concat($data, $subquery_results) |> Vec\unique_by($$, $row_encoder);
          break;
        case MultiOperand::UNION_ALL:
          // just concatenate with no uniqueness
          $data = Vec\concat($data, $subquery_results);
          break;
        case MultiOperand::INTERSECT:
          // there's no Vec\intersect_by currently
          $encoded_data = Keyset\map($data, $row_encoder);
          $data = Vec\filter($subquery_results, $row ==> C\contains_key($encoded_data, $row_encoder($row)));
          break;
        case MultiOperand::EXCEPT:
          $data = Vec\diff_by($data, $subquery_results, $row_encoder);
          break;
      }
    }

    return $data;
  }

}
