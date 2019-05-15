<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Str};

// extracts a column from a row by index
final class ColumnExpression extends Expression {
  private string $columnExpression;
  private string $columnName;
  private ?string $tableName;
  private ?string $databaseName;
  private bool $allowFallthrough = false;


  public function __construct(token $token) {
    $this->type = $token['type'];
    $this->precedence = 0;

    $this->columnExpression = $token['value'];
    $this->columnName = $token['value'];

    # TODO handle database schema here
    if (Str\contains($token['value'], '.')) {
      $parts = Str\split($token['value'], '.');
      if (C\count($parts) === 2) {
        list($this->tableName, $this->columnName) = $parts;
      } elseif (C\count($parts) === 3) {
        list($this->databaseName, $this->tableName, $this->columnName) = $parts;
      }
    } else {
      $this->tableName = null;
    }

    if ($token['value'] === '*') {
      $this->name = '*';
      return;
    }

    $this->name = $this->columnName;

    /* TODO turn this on once we have DB schemas in place
    // Most of the time,
    $found = false;
    if ($index === -1){
    	foreach ($columns as $index => $col){
    		if (($table === null || $col['table'] === $table) && $col['column'] === $column){
    			// if we already found a match for this column and then find another, it's an error for it to be ambiguous
    			if ($found) throw new DBMockParseException("Column {$column} in field list is ambiguous");
    			$this->index = $index;
    			$found = true;
    		}
    	}
    }

    if (!$found) throw new DBMockParseException("Column {$column} not found on table {$table}");
    */
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    // for the "COUNT(*)" case, just return 1
    // we don't actually implement "*" in this library, the select processer handles that
    if ($this->name === '*')
      return 1;

    $row = $this->maybeUnrollGroupedDataset($row);

    // otherwise return the column
    if (C\contains_key($row, $this->columnExpression)) {
      return $row[$this->columnExpression];
    } elseif (($this->tableName === null && $this->columnName is nonnull) || $this->allowFallthrough) {
      // didn't find row by alias, so search without alias instead
      // but only if the column expression didn't have an explicit table name on it
      // OR if we are explicitly allowing fallthrough to the full row, which we do in the ORDER BY clause
      foreach ($row as $key => $col) {
        if (C\lastx(Str\split($key, '.')) === $this->columnName) {
          return $col;
        }
      }
    }

    if (C\contains_key($row, $this->name)) {
      return $row[$this->name];
    }

    if (QueryContext::$strictMode) {
      // we've running in strict mode but we still ran into a column that was missing.
      // this means we're selecting on a column that does not exist
      throw new DBMockRuntimeException("Column with index ".$this->columnExpression." not found in row");
    } else {
      return null;
    }
  }

  /**
   * for use in ORDER BY... allow evaluating the expression
   * to fall through to the full row if the column is not found fully qualified.
   */
  public function allowFallthrough(): void {
    $this->allowFallthrough = true;
  }

  public function isWellFormed(): bool {
    return true;
  }

  public function tableName(): ?string {
    return $this->tableName;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, string> {
    return dict['type' => 'colref', 'name' => $this->name];
  }
}
