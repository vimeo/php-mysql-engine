<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

/**
 * This can only be used in ORDER BY, it's for expressions like "ORDER BY 2 DESC"
 * it refers to a position of a column in the select list
 */
final class PositionExpression extends Expression {
  public function __construct(private int $position) {
    $this->type = TokenType::IDENTIFIER;
    $this->precedence = 0;
    $this->name = (string)$position;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    // unroll GROUP BY. if passed a grouped data set, use the very first row
    if (C\first($row) is dict<_, _>) {
      $row = C\firstx($row) as dict<_, _>;
    }

    // SQL positions are 1-indexed, dicts are 0-indexed
    if (!C\contains_key($row, $this->position - 1)) {
      throw new DBMockRuntimeException("Undefined positional reference {$this->position} IN GROUP BY or ORDER BY");
    }
    return $row[$this->position - 1];
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return true;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    return dict['type' => 'position', 'position' => $this->position];
  }

}
