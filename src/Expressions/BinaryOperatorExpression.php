<?hh // strict

/* HHAST_IGNORE_ALL[NoPHPEquality] */

namespace Slack\DBMock;

use namespace HH\Lib\{C, Regex, Str};

/**
 * any operator that takes arguments on the left and right side, like +, -, *, AND, OR...
 */
final class BinaryOperatorExpression extends Expression {

  protected bool $evaluates_groups = false;
  protected int $negatedInt = 0;

  public function __construct(
    public Expression $left, // public because we sometimes need to access it to split off into a BETWEEN
    public bool $negated = false,
    public string $operator = '',
    public ?Expression $right = null,
  ) {
    $this->name = '';
    // this gets overwritten once we have an operator
    $this->precedence = 0;
    $this->type = TokenType::OPERATOR;
    if ($operator) {
      $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
    }

    $this->negatedInt = $this->negated ? 1 : 0;
  }

  /**
   * Runs the comparison on each element between the left and right,
   * BUT if the values are equal it keeps checking down the list
   * (1, 2, 3) > (1, 2, 2) for example
   * (1, 2, 3) > (1, 1, 4) is also true
   */
  private function evaluateRowComparison(
    RowExpression $left,
    RowExpression $right,
    row $row,
    AsyncMysqlConnection $conn,
  ): bool {

    $left_elems = $left->evaluate($row, $conn);
    invariant($left_elems is vec<_>, "RowExpression must return vec");

    $right_elems = $right->evaluate($row, $conn);
    invariant($right_elems is vec<_>, "RowExpression must return vec");

    if (C\count($left_elems) !== C\count($right_elems)) {
      throw new DBMockRuntimeException("Mismatched column count in row comparison expression");
    }

    $last_index = C\last_key($left_elems);
    $match = true;

    foreach ($left_elems as $index => $le) {
      $re = $right_elems[$index];

      // in an expression like (1, 2, 3) > (1, 2, 2) we don't need EVERY element on the left to be greater than the right
      // some can be equal. so if we get to one that isn't the last and they're equal, it's safe to keep going
      if ($le == $re && $index !== $last_index) {
        continue;
      }

      // as soon as you find any pair of elements that aren't equal, you can return whatever their comparison result is immediately
      // this is why (1, 2, 3) > (1, 1, 4) is true, for example, because the 2nd element comparison returns immediately
      switch ($this->operator) {
        case '=':
          return ($le == $re);
        case '<>':
        case '!=':
          return ($le != $re);
        case '>':
          /* HH_IGNORE_ERROR[4240] assume they have the same types */
          return ($le > $re);
        case '>=':
          /* HH_IGNORE_ERROR[4240] assume they have the same types */
          return ($le >= $re);
        case '<':
          /* HH_IGNORE_ERROR[4240] assume they have the same types */
          return ($le < $re);
        case '<=':
          /* HH_IGNORE_ERROR[4240] assume they have the same types */
          return ($le <= $re);
        default:
          throw new DBMockRuntimeException("Operand {$this->operator} should contain 1 column(s)");
      }
    }

    return false;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
    $right = $this->right;
    $left = $this->left;

    if ($left is RowExpression) {
      if (!$right is RowExpression) {
        throw new DBMockRuntimeException("Expected row expression on RHS of {$this->operator} operand");
      }

      // oh fun! a row comparison, e.g. (col1, col2, col3) > (1, 2, 3)
      // these are handled somewhat differently from all other binary operands since you need to loop and compare each element
      // also we cast to int because that's how MySQL would return these
      return (int)($this->evaluateRowComparison($left, $right, $row, $conn));
    }

    if ($right === null) {
      throw new DBMockRuntimeException("Attempted to evaluate BinaryOperatorExpression with no right operand");
    }

    $l_value = $left->evaluate($row, $conn);
    $r_value = $right->evaluate($row, $conn);

    $as_string = $left->getType() == TokenType::STRING_CONSTANT || $right->getType() == TokenType::STRING_CONSTANT;

    switch ($this->operator) {
      case '':
        // an operator should only be in this state in the middle of parsing, never when evaluating
        throw new DBMockRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');
      case 'AND':
        if ((bool)$l_value && (bool)$r_value) {
          return (int)!$this->negated;
        }
        return (int)$this->negated;
      case 'OR':
        if ((bool)$l_value || (bool)$r_value) {
          return (int)!$this->negated;
        }
        return (int)$this->negated;
      case '=':
        // maybe do some stuff with data types here
        // comparing strings: gotta think about collation and case sensitivity!
        return ($l_value == $r_value) ? 1 : 0 ^ $this->negatedInt;
      case '<>':
      case '!=':
        if ($as_string) {
          return ((string)$l_value != (string)$r_value) ? 1 : 0 ^ $this->negatedInt;
        } else {
          return ((int)$l_value != (int)$r_value) ? 1 : 0 ^ $this->negatedInt;
        }
      case '>':
        if ($as_string) {
          return ((string)$l_value > (string)$r_value) ? 1 : 0 ^ $this->negatedInt;
        } else {
          return ((int)$l_value > (int)$r_value) ? 1 : 0 ^ $this->negatedInt;
        }
      case '>=':
        if ($as_string) {
          return ((string)$l_value >= (string)$r_value) ? 1 : 0 ^ $this->negatedInt;
        } else {
          return ((int)$l_value >= (int)$r_value) ? 1 : 0 ^ $this->negatedInt;
        }
      case '<':
        if ($as_string) {
          return ((string)$l_value < (string)$r_value) ? 1 : 0 ^ $this->negatedInt;
        } else {
          return ((int)$l_value < (int)$r_value) ? 1 : 0 ^ $this->negatedInt;
        }
      case '<=':
        if ($as_string) {
          return ((string)$l_value <= (string)$r_value) ? 1 : 0 ^ $this->negatedInt;
        } else {
          return ((int)$l_value <= (int)$r_value) ? 1 : 0 ^ $this->negatedInt;
        }
      case '*':
      case '%':
      case 'MOD':
      case '-':
      case '+':
      case '<<':
      case '>>':
      case '*':
      case '/':
      case 'DIV':
        // do these things to all numeric operators and then switch again to execute the actual operation
        $left_number = $this->extractNumericValue($l_value);
        $right_number = $this->extractNumericValue($r_value);

        switch ($this->operator) {
          case '*':
            return $left_number * $right_number;
          case '%':
          case 'MOD':
            // mod is float-aware, not ints only like PHP's % operator
            return \fmod($left_number, $right_number);
          case '/':
            return $left_number / $right_number;
          case 'DIV':
            // integer division
            return (int)($left_number / $right_number);
          case '-':
            return $left_number - $right_number;
          case '+':
            return $left_number + $right_number;
          case '<<':
            return (int)$left_number << (int)$right_number;
          case '>>':
            return (int)$left_number >> (int)$right_number;

        }
      case 'LIKE':
        $left_string = (string)$left->evaluate($row, $conn);
        if (!$right is ConstantExpression) {
          throw new DBMockRuntimeException("LIKE pattern should be a constant string");
        }
        $pattern = (string)$r_value;

        $start_pattern = '^';
        $end_pattern = '$';

        if ($pattern[0] == '%') {
          $start_pattern = '';
          $pattern = Str\strip_prefix($pattern, '%');
        }

        if (Str\ends_with($pattern, '%')) {
          $end_pattern = '';
          $pattern = Str\strip_suffix($pattern, '%');
        }

        // replace only unescaped % and _ characters to make regex
        $pattern = Regex\replace($pattern, re"/(?<!\\\)%/", '.*?');
        $pattern = Regex\replace($pattern, re"/(?<!\\\)_/", '.');

        $regex = '/'.$start_pattern.$pattern.$end_pattern.'/s';

        // xor here, so if negated is true and regex matches then we return false
        return ((bool)\preg_match($regex, $left_string) ? 1 : 0) ^ $this->negatedInt;
      case 'IS':
        if (!$right is ConstantExpression) {
          throw new DBMockRuntimeException("Unsupported right operand for IS keyword");
        }
        $val = $left->evaluate($row, $conn);

        $r = $r_value;

        if ($r === null) {
          return ($val === null ? 1 : 0) ^ $this->negatedInt;
        }

        // you can also do IS TRUE, IS FALSE, or IS UNKNOWN but I haven't implemented that yet mostly because those come through the parser as "RESERVED" rather than const expressions

        throw new DBMockRuntimeException("Unsupported right operand for IS keyword");
      case 'RLIKE':
      case 'REGEXP':
        $left_string = (string)$left->evaluate($row, $conn);
        // if the regexp is wrapped in a BINARY function we will make it case sensitive
        $case_insensitive = 'i';
        if ($right is FunctionExpression && $right->functionName() == 'BINARY') {
          $case_insensitive = '';
        }

        $pattern = (string)$r_value;
        $regex = '/'.$pattern.'/'.$case_insensitive;

        // xor here, so if negated is true and regex matches then we return false etc.
        return ((bool)\preg_match($regex, $left_string) ? 1 : 0) ^ $this->negatedInt;
      case '&&':
      case 'BINARY':
      case 'COLLATE':
      case '&':
      case '|':
      case '^':
      case '<=>':
      case '||':
      case 'XOR':
      case 'SOUNDS':
      case 'ANY': // parser does NOT KNOW about this functionality
      case 'SOME': // parser does NOT KNOW about this functionality
      default:
        throw new DBMockRuntimeException("Operator {$this->operator} not implemented in DB Mock");
    }
  }

  /**
   * Coerce a mixed value to a num,
   * but also handle sub-expressions that return a dataset containing a num
   * such as "SELECT (SELECT COUNT(*) FROM ...) + 3 as thecountplusthree"
   */
  protected function extractNumericValue(mixed $val): num {
    if ($val is Container<_>) {
      if (C\is_empty($val)) {
        $val = 0;
      } else {
        // extract first row, then first column
        $val = (C\firstx($val) as Container<_>) |> C\firstx($$);
      }
    }
    return Str\contains((string)$val, '.') ? (float)$val : (int)$val;
  }

  <<__Override>>
  public function negate(): void {
    $this->negated = true;
    $this->negatedInt = 1;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {

    $ret = dict[
      'type' => $this->operator,
      'left' => \var_dump($this->left, true),
      'right' => $this->right ? \var_dump($this->right, true) : dict[],
    ];

    if ($this->name) {
      $ret['name'] = $this->name;
    }
    if ($this->negated) {
      $ret['negated'] = 1;
    }
    return $ret;
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return $this->right && $this->operator;
  }

  <<__Override>>
  public function setNextChild(Expression $expr, bool $overwrite = false): void {
    if (!$this->operator || ($this->right && !$overwrite)) {
      throw new DBMockParseException("Parse error");
    }
    $this->right = $expr;
  }

  public function setOperator(string $operator): void {
    $this->operator = $operator;
    $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[$operator];
  }

  public function getRightOrThrow(): Expression {
    if ($this->right === null) {
      throw new DBMockParseException("Parse error: attempted to resolve unbound expression");
    }
    return $this->right;
  }

  <<__Override>>
  public function addRecursiveExpression(token_list $tokens, int $pointer, bool $negated = false): int {
    // this might not end up as a binary expression, but it is ok for it to start that way!
    // $right could be empty here if we encountered an expression on the right hand side of an operator like, "column_name = CASE.."
    $tmp = $this->right ? new BinaryOperatorExpression($this->right) : new PlaceholderExpression();

    // what we want to do is tell the child to process itself until it finds a precedence lower than the parent
    $p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, true);
    list($pointer, $new_expression) = $p->buildWithPointer();

    if ($negated) {
      $new_expression->negate();
    }

    $this->setNextChild($new_expression, true);

    return $pointer;
  }
}
