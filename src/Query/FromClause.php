<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

/**
 * Represents the entire FROM clause of a query,
 * built up incrementally when parsing.
 *
 * Contains zero or more from_table expressions, order matters
 */
final class FromClause {

  public vec<from_table> $tables = vec[];
  public bool $mostRecentHasAlias = false;

  public function addTable(from_table $table): void {
    $this->tables[] = $table;
    $this->mostRecentHasAlias = false;
  }

  public function aliasRecentExpression(string $name): void {
    $k = C\last_key($this->tables);
    if ($k === null || $this->mostRecentHasAlias) {
      throw new SQLFakeParseException("Unexpected AS");
    }
    $this->tables[$k]['alias'] = $name;
    $this->mostRecentHasAlias = true;
  }

  /**
   * The FROM clause of the query gets processed first, retrieving data from tables, executing subqueries, and handling joins
   * This is also where we build up the $columns list which is commonly used throughout the entire library to map column references to indexes in this dataset
   */
  public function process(AsyncMysqlConnection $conn, string $sql): dataset {

    $data = vec[];
    $columns = vec[];
    $is_first_table = true;
    $left_column_list = keyset[];

    foreach ($this->tables as $table) {

      $schema = null;
      if (Shapes::keyExists($table, 'subquery')) {
        $res = $table['subquery']->evaluate(dict[], $conn);
        $name = $table['name'];
      } else {
        $table_name = $table['name'];

        list($database, $table_name) = Query::parseTableName($conn, $table_name);

        // TODO if different database, should $name have that in it as well for other things like column references? probably, right?
        $name = $table['alias'] ?? $table_name;
        $schema = QueryContext::getSchema($database, $table_name);
        if ($schema === null && QueryContext::$strictSchemaMode) {
          throw new SQLFakeRuntimeException("Table $table_name not found in schema and strict mode is enabled");
        }

        $res = $conn->getServer()->getTable($database, $table_name);

        if ($res === null) {
          $res = vec[];
        }
      }

      $new_dataset = vec[];
      if ($schema !== null) {
        // if schema is set, order the fields in the right order on each row
        $ordered_fields = keyset[];
        foreach ($schema['fields'] as $field) {
          $ordered_fields[] = $field['name'];
        }

        foreach ($res as $row) {
          $m = dict[];
          foreach ($ordered_fields as $field) {
            if (!C\contains_key($row, $field)) {
              continue;
            }
            $m["{$name}.{$field}"] = $row[$field];
          }
          $new_dataset[] = $m;
        }
      } else {
        foreach ($res as $row) {
          $m = dict[];
          foreach ($row as $key => $val) {
            $m["{$name}.{$key}"] = $val;
          }
          $new_dataset[] = $m;
        }
      }

      if ($data || !$is_first_table) {
        // do the join here. based on join type, pass in $data and $res to filter. and aliases
        $data = JoinProcessor::process(
          $conn,
          $data,
          $new_dataset,
          $name,
          $table['join_type'],
          $table['join_operator'] ?? null,
          $table['join_expression'] ?? null,
          $schema,
        );
      } else {
        $data = $new_dataset;
      }

      if ($is_first_table) {
        Metrics::trackQuery(QueryType::SELECT, $conn->getServer()->name, $name, $sql);
        $is_first_table = false;
      }
    }

    return $data;
  }
}
