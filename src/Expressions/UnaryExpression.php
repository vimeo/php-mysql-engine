<?hh // strict

namespace Slack\DBMock;

/**
 * Represents a unary operator (one that takes only one argument)
 * Examples: SELECT -1 from mytable where column=-1
 * SELECT ~column
 * SELECT +5
 */
final class UnaryExpression extends Expression {

  private ?Expression $subject = null;

  public function __construct(public string $operator) {
    $this->type = TokenType::OPERATOR;
    $this->precedence = 14;
    $this->name = $operator;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    if ($this->subject === null) {
      throw new DBMockRuntimeException("Attempted to evaluate unary operation with no operand");
    }
    $val = $this->subject->evaluate($row, $conn);
    switch ($this->operator) {
      case 'UNARY_MINUS':
        return -1 * (float)$val;
      case 'UNARY_PLUS':
        return (float)$val;
      case '~':
        return ~(int)$val;
      default:
        throw new DBMockRuntimeException("Unimplemented unary operand {$this->name}");
    }

    return $val;
  }

  <<__Override>>
  public function setNextChild(Expression $expr, bool $overwrite = false): void {
    if ($this->subject is nonnull && !$overwrite) {
      throw new DBMockParseException("Unexpected expression after unary operand");
    }
    $this->subject = $expr;
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return $this->subject is nonnull;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    $subject = $this->subject ? \var_dump($this->subject, true) : dict[];
    return dict[
      'type' => 'unary',
      'operator' => $this->operator,
      'name' => $this->name,
      'subject' => $subject,
    ];
  }
}
