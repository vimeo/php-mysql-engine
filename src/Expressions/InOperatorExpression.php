<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str};

/**
 * a statement like X IN (1, 2, 3)
 */
final class InOperatorExpression extends Expression {

  private ?vec<Expression> $inList = null;

  public function __construct(private Expression $left, public bool $negated = false) {
    $this->name = '';
    $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['IN'];
    $this->operator = 'IN';
    $this->type = TokenType::OPERATOR;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    $inList = $this->inList;
    if ($inList === null || C\count($inList) === 0) {
      throw new SQLFakeParseException("Parse error: empty IN list");
    }

    //
    // Handle NULL as a special case: MySQL evaluates both "IN (NULL)" and "NOT IN (NULL)" to false,
    // but while "IN (NULL)" might make sense when running a query with an empty IN list,
    // "NOT IN (NULL)" almost certainly doesn't match what the developer is expecting.
    // To avoid confusion, we just throw an SQLFakeException here.
    //
    if (C\count($inList) === 1 && $inList[0]->evaluate($row, $conn) === null) {
      if (!$this->negated) {
        return false;
      } else {
        throw new SQLFakeRuntimeException(
          "You're probably trying to use NOT IN with an empty array, but MySQL would evaluate this to false.",
        );
      }
    }

    $value = $this->left->evaluate($row, $conn);
    foreach ($inList as $in_expr) {
      // found it? return the opposite of "negated". so if negated is false, return true.
      // if it's a subquery, we have to iterate over the results and extract the field from each row
      if ($in_expr is SubqueryExpression) {
        $ret = $in_expr->evaluate($row, $conn) as KeyedContainer<_, _>;
        foreach ($ret as $r) {
          $r as KeyedContainer<_, _>;
          if (C\count($r) !== 1) {
            throw new SQLFakeRuntimeException("Subquery result should contain 1 column");
          }
          foreach ($r as $val) {
            if ($value == $val) {
              return !$this->negated;
            }
          }
        }
      } else {
        if ($value == $in_expr->evaluate($row, $conn)) {
          return !$this->negated;
        }
      }
    }

    return $this->negated;
  }

  <<__Override>>
  public function negate(): void {
    $this->negated = true;
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return $this->inList !== null;
  }

  public function setInList(vec<Expression> $list): void {
    $this->inList = $list;
  }

  <<__Override>>
  public function setNextChild(Expression $expr, bool $_overwrite = false): void {
    $this->inList = vec[$expr];
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    $inList = vec[];
    if ($this->inList !== null) {
      foreach ($this->inList as $expr) {
        $inList[] = \var_dump($expr, true);
      }
    }
    $ret = dict[
      'type' => 'IN',
      'left' => \var_dump($this->left, true),
      'in' => $inList,
      'negated' => $this->negated,
    ];

    if (!Str\is_empty($this->name)) {
      $ret['name'] = $this->name;
    }
    return $ret;
  }
}
