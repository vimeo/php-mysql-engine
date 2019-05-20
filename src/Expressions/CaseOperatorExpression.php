<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

/**
 * a statement like CASE WHEN X THEN Y ELSE Z END
 */
final class CaseOperatorExpression extends Expression {

  private vec<shape(
    'when' => Expression,
    'then' => Expression,
  )> $whenExpressions = vec[];

  private ?Expression $when;
  private ?Expression $then;
  private ?Expression $else;
  private string $lastKeyword = 'CASE';
  private bool $wellFormed = false;

  public function __construct(token $_token) {
    $this->name = 'CASE';
    $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['CASE'];
    $this->operator = 'CASE';
    $this->type = TokenType::OPERATOR;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    if (!$this->wellFormed) {
      throw new SQLFakeRuntimeException("Attempted to evaluate incomplete CASE expression");
    }

    foreach ($this->whenExpressions as $clause) {
      if ((bool)$clause['when']->evaluate($row, $conn)) {
        return $clause['then']->evaluate($row, $conn);
      }
    }

    invariant($this->else is nonnull, 'must have else since wellFormed was true');
    return $this->else->evaluate($row, $conn);
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return $this->wellFormed;
  }

  public function setKeyword(string $keyword): void {
    switch ($keyword) {
      case 'WHEN':
        if ($this->lastKeyword !== 'CASE' && $this->lastKeyword !== 'THEN') {
          throw new SQLFakeParseException("Unexpected WHEN in CASE statement");
        }
        $this->lastKeyword = 'WHEN';
        // set these to null in case this is not the first WHEN clause, so that the clauses know to accept expressions
        $this->when = null;
        $this->then = null;
        break;
      case 'THEN':
        if ($this->lastKeyword !== 'WHEN' || !$this->when) {
          throw new SQLFakeParseException("Unexpected THEN in CASE statement");
        }
        $this->lastKeyword = 'THEN';
        break;
      case 'ELSE':
        if ($this->lastKeyword !== 'THEN' || !$this->then) {
          throw new SQLFakeParseException("Unexpected ELSE in CASE statement");
        }
        $this->lastKeyword = 'ELSE';
        break;
      case 'END':
        // ELSE clause is optional, it becomes an "ELSE NULL" implicitly if not present
        if ($this->lastKeyword === 'THEN' && $this->then) {
          $this->else = new ConstantExpression(shape(
            'type' => TokenType::NULL_CONSTANT,
            'value' => 'null',
            'raw' => 'null',
          ));
        } elseif ($this->lastKeyword !== 'ELSE' || !$this->else) {
          throw new SQLFakeParseException("Unexpected END in CASE statement");
        }
        $this->lastKeyword = 'END';
        $this->wellFormed = true;
        break;
      default:
        throw new SQLFakeParseException("Unexpected keyword $keyword in CASE statement");
    }
  }

  <<__Override>>
  public function setNextChild(Expression $expr, bool $overwrite = false): void {
    switch ($this->lastKeyword) {
      case 'CASE':
        throw new SQLFakeParseException("Missing WHEN in CASE");
      case 'WHEN':
        if ($this->when && !$overwrite) {
          throw new SQLFakeParseException("Unexpected token near WHEN");
        }
        $this->when = $expr;
        break;
      case 'THEN':
        if ($this->then && !$overwrite) {
          throw new SQLFakeParseException("Unexpected token near THEN");
        }
        $this->then = $expr;
        $this->whenExpressions[] = shape('when' => $this->when as nonnull, 'then' => $expr);
        break;
      case 'ELSE':
        if ($this->else && !$overwrite) {
          throw new SQLFakeParseException("Unexpected token near ELSE");
        }
        $this->else = $expr;
        break;
      case 'END':
        throw new SQLFakeParseException("Unexpected token near END");
    }
  }

  <<__Override>>
  public function addRecursiveExpression(token_list $tokens, int $pointer, bool $negated = false): int {
    $p = new ExpressionParser($tokens, $pointer, new PlaceholderExpression(), 0, true);
    list($pointer, $new_expression) = $p->buildWithPointer();

    if ($negated) {
      $new_expression->negate();
    }

    // the way case statements are parsed... we actually do not want to overwrite
    $this->setNextChild($new_expression, false);
    return $pointer;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    $last_case = C\lastx($this->whenExpressions);
    $when_list = vec[];
    foreach ($this->whenExpressions as $exp) {
      $when_list[] = dict['when' => \var_dump($exp['when'], true), 'then' => \var_dump($exp['then'], true)];
    }
    $ret = dict[
      'type' => (string)$this->type,
      'whenExpressions' => $when_list,
      'else' => $this->else ? \var_dump($this->else, true) : dict[],
    ];

    if ($this->name) {
      $ret['name'] = $this->name;
    }
    return $ret;
  }
}
