<?hh // strict

namespace Slack\SQLFake;

/**
 * Represents a statement like X BETWEEN Y AND Z
 */
final class BetweenOperatorExpression extends Expression {

  private ?Expression $start = null;
  private ?Expression $end = null;
  private bool $and = false;
  protected bool $evaluates_groups = false;
  public bool $negated = false;

  public function __construct(private Expression $left) {
    $this->name = '';
    $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['BETWEEN'];
    $this->operator = 'BETWEEN';
    $this->type = TokenType::OPERATOR;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    $start = $this->start;
    $end = $this->end;
    if ($start === null || $end === null) {
      throw new SQLFakeRuntimeException("Attempted to evaluate incomplete BETWEEN expression");
    }

    // any part of the between clause could be a column or a literal, so check each one
    $subject = $this->left->evaluate($row, $conn);
    $start = $start->evaluate($row, $conn);
    $end = $end->evaluate($row, $conn);

    // between clause is lower and upper inclusive
    if ($subject is num) {
      $subject = (int)$subject;
      $start = (int)$start;
      $end = (int)$end;
      $eval = $subject >= $start && $subject <= $end;
    } else {
      $subject = (string)$subject;
      $start = (string)$start;
      $end = (string)$end;
      $eval = $subject >= $start && $subject <= $end;
    }

    return ($this->negated ? !$eval : $eval) ? 1 : 0;
  }

  <<__Override>>
  public function negate(): void {
    $this->negated = true;
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return $this->start && $this->end;
  }

  public function setStart(Expression $expr): void {
    $this->start = $expr;
  }

  public function setEnd(Expression $expr): void {
    $this->end = $expr;
  }

  public function foundAnd(): void {
    if ($this->and || !$this->start) {
      throw new SQLFakeParseException("Unexpected AND");
    }
    $this->and = true;
  }

  <<__Override>>
  public function setNextChild(Expression $expr, bool $overwrite = false): void {
    if ($overwrite) {
      // this mode is when we come out of a recursive expression where we had to pull out the most recent token, so overwrite that expression with the result
      if ($this->end) {
        $this->end = $expr;
      } elseif ($this->start) {
        $this->start = $expr;
      } else {
        $this->left = $expr;
      }
      return;
    }

    if (!$this->start) {
      $this->start = $expr;
    } elseif ($this->and && !$this->end) {
      $this->end = $expr;
    } else {
      throw new SQLFakeParseException("Parse error: unexpected token in BETWEEN statement");
    }
  }

  private function getLatestExpression(): Expression {
    if ($this->end) {
      return $this->end;
    }
    if ($this->start) {
      return $this->start;
    }
    return $this->left;
  }

  <<__Override>>
  public function addRecursiveExpression(token_list $tokens, int $pointer, bool $negated = false): int {

    $tmp = new BinaryOperatorExpression($this->getLatestExpression());

    $p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, /* $is_child */ true);
    list($pointer, $new_expression) = $p->buildWithPointer();

    if ($negated) {
      $new_expression->negate();
    }

    $this->setNextChild($new_expression, true);

    return $pointer;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    $ret = dict[
      'type' => (string)$this->type,
      'left' => \var_dump($this->left, true),
      'start' => $this->start ? \var_dump($this->start, true) : dict[],
      'end' => $this->end ? \var_dump($this->end, true) : dict[],
    ];

    if ($this->name) {
      $ret['name'] = $this->name;
    }
    return $ret;
  }
}
