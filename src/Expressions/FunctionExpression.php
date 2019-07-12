<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Math, Str};

/**
 * emulates a call to a built-in MySQL function
 * we implement as many as we want to in Hack
 */
final class FunctionExpression extends Expression {

  private string $functionName;
  protected bool $evaluatesGroups = true;

  public function __construct(private token $token, private vec<Expression> $args, private bool $distinct) {
    $this->type = $token['type'];
    $this->precedence = 0;
    $this->functionName = $token['value'];
    $this->name = $token['value'];
    $this->operator = (string)$this->type;
  }

  <<__Override>>
  public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {

    switch ($this->functionName) {
      case 'COUNT':
        return $this->sqlCount($row, $conn);
      case 'SUM':
        return $this->sqlSum($row, $conn);
      case 'MAX':
        return $this->sqlMax($row, $conn);
      case 'MIN':
        return $this->sqlMin($row, $conn);
      case 'MOD':
        return $this->sqlMod($row, $conn);
      case 'AVG':
        return $this->sqlAvg($row, $conn);
      case 'IF':
        return $this->sqlIf($row, $conn);
      case 'IFNULL':
      case 'COALESCE':
        return $this->sqlCoalesce($row, $conn);
      case 'NULLIF':
        return $this->sqlNullif($row, $conn);
      case 'SUBSTRING':
      case 'SUBSTR':
        return $this->sqlSubstring($row, $conn);
      case 'SUBSTRING_INDEX':
        return $this->sqlSubstringIndex($row, $conn);
      case 'LENGTH':
        return $this->sqlLength($row, $conn);
      case 'LOWER':
        return $this->sqlLower($row, $conn);
      case 'CHAR_LENGTH':
      case 'CHARACTER_LENGTH':
        return $this->sqlCharLength($row, $conn);
      case 'CONCAT_WS':
        return $this->sqlConcatWS($row, $conn);
      case 'CONCAT':
        return $this->sqlConcat($row, $conn);
      case 'FIELD':
        return $this->sqlField($row, $conn);
      case 'BINARY':
        return $this->sqlBinary($row, $conn);
      case 'FROM_UNIXTIME':
        return $this->sqlFromUnixtime($row, $conn);
      case 'GREATEST':
        return $this->sqlGreatest($row, $conn);
      case 'VALUES':
        return $this->sqlValues($row, $conn);

      // GROUP_CONCAT might be a nice one to implement but it does have a lot of params and isn't really used in our codebase
    }

    throw new SQLFakeRuntimeException("Function ".$this->functionName." not implemented yet");
  }

  public function functionName(): string {
    return $this->functionName;
  }

  public function isAggregate(): bool {
    return C\contains_key(keyset['COUNT', 'SUM', 'MIN', 'MAX', 'AVG'], $this->functionName);
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return true;
  }

  /**
   * helper for functions which take one expression as an argument
   */
  private function getExpr(): Expression {
    invariant(C\count($this->args) === 1, 'expression must have one argument');
    return C\firstx($this->args);
  }

  private function sqlCount(row $rows, AsyncMysqlConnection $conn): int {
    $expr = $this->getExpr();

    if ($this->distinct) {
      $buckets = dict[];
      foreach ($rows as $row) {
        $row as dict<_, _>;
        $val = $expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn);
        if (!C\contains_key($buckets, $val)) {
          $buckets[$val] = 1;
        }
      }

      return C\count($buckets);
    }

    $count = 0;
    foreach ($rows as $row) {
      // all functions are passed a row object
      // but select process will pass groups of rows instead so each element should be an entire row
      $row as dict<_, _>;
      if ($expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn) is nonnull) {
        $count++;
      }
    }

    return $count;
  }

  private function sqlSum(row $rows, AsyncMysqlConnection $conn): num {
    $expr = $this->getExpr();
    $sum = 0;

    foreach ($rows as $row) {
      $row as dict<_, _>;
      $val = $expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn);
      $num = $val is int ? $val : (float)($val);
      $sum += $num;
    }
    return $sum;
  }

  private function sqlMin(row $rows, AsyncMysqlConnection $conn): mixed {
    $expr = $this->getExpr();
    $values = vec[];
    foreach ($rows as $row) {
      $row as dict<_, _>;
      $values[] = $expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn);
    }

    if (C\is_empty($values)) {
      return null;
    }

    return \min($values);
  }

  private function sqlMax(row $rows, AsyncMysqlConnection $conn): mixed {
    $expr = $this->getExpr();

    $values = vec[];
    foreach ($rows as $row) {
      $row as dict<_, _>;
      $values[] = $expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn);
    }

    if (C\is_empty($values)) {
      return null;
    }

    return \max($values);
  }

  private function sqlMod(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 2) {
      throw new SQLFakeRuntimeException("MySQL MOD() function must be called with two arguments");
    }
    $n = $args[0];
    $n_value = (int)$n->evaluate($row, $conn);
    $m = $args[1];
    $m_value = (int)$m->evaluate($row, $conn);

    return $n_value % $m_value;
  }

  private function sqlAvg(row $rows, AsyncMysqlConnection $conn): mixed {
    $expr = $this->getExpr();

    $values = vec[];
    foreach ($rows as $row) {
      $row as dict<_, _>;
      $values[] = $expr->evaluate(/* HH_FIXME[4110] generics */ $row, $conn) as num;
    }

    if (C\is_empty($values)) {
      return null;
    }

    return Math\mean($values);
  }

  private function sqlIf(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 3) {
      throw new SQLFakeRuntimeException("MySQL IF() function must be called with three arguments");
    }
    $condition = $args[0];

    // evaluate the ELSE condition, unless the IF condition is true
    $arg_to_evaluate = 2;
    if ((bool)$condition->evaluate($row, $conn)) {
      $arg_to_evaluate = 1;
    }
    $expr = $args[$arg_to_evaluate];
    return $expr->evaluate($row, $conn);
  }

  private function sqlSubstring(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 2 && C\count($args) !== 3) {
      throw new SQLFakeRuntimeException("MySQL SUBSTRING() function must be called with two or three arguments");
    }
    $subject = $args[0];
    $string = (string)$subject->evaluate($row, $conn);

    $position = $args[1];
    $pos = (int)$position->evaluate($row, $conn);

    // MySQL string positions 1-indexed, PHP strings are 0-indexed. So substract one from pos
    $pos -= 1;

    $length = $args[2] ?? null;
    if ($length !== null) {
      $len = (int)$length->evaluate($row, $conn);
      return \mb_substr($string, $pos, $len);
    }

    return \mb_substr($string, $pos);
  }

  private function sqlSubstringIndex(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 3) {
      throw new SQLFakeRuntimeException("MySQL SUBSTRING_INDEX() function must be called with three arguments");
    }
    $subject = $args[0];
    $string = (string)$subject->evaluate($row, $conn);

    $delimiter = $args[1];
    $delim = (string)$delimiter->evaluate($row, $conn);

    // MySQL string positions 1-indexed, PHP strings are 0-indexed. So substract one from pos
    $pos = $args[2];
    if ($pos is nonnull){
      $count = (int)$pos->evaluate($row, $conn);
      $parts = Str\split($string, $delim);
      if ($count < 0){
        $slice = \array_slice($parts, $count);
      } else {
        $slice = \array_slice($parts, 0, $count);
      }

      return Str\join($slice, $delim);
    }
    return '';
  }

  private function sqlLower(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 1) {
      throw new SQLFakeRuntimeException("MySQL LOWER() function must be called with one argument");
    }
    $subject = $args[0];
    $string = (string)$subject->evaluate($row, $conn);

    return Str\lowercase($string);
  }

  private function sqlLength(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 1) {
      throw new SQLFakeRuntimeException("MySQL LENGTH() function must be called with one argument");
    }
    $subject = $args[0];
    $string = (string)$subject->evaluate($row, $conn);

    return Str\length($string);
  }

  private function sqlBinary(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 1) {
      throw new SQLFakeRuntimeException("MySQL BINARY() function must be called with one argument");
    }
    $subject = $args[0];
    return $subject->evaluate($row, $conn);
  }

  private function sqlCharLength(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 1) {
      throw new SQLFakeRuntimeException("MySQL CHAR_LENGTH() function must be called with one argument");
    }
    $subject = $args[0];
    $string = (string)$subject->evaluate($row, $conn);

    return \mb_strlen($string);
  }

  private function sqlCoalesce(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    if (!C\count($this->args)) {
      throw new SQLFakeRuntimeException("MySQL COALESCE() function must be called with at least one argument");
    }

    foreach ($this->args as $arg) {
      $val = $arg->evaluate($row, $conn);
      if ($val is nonnull) {
        return $val;
      }
    }
    return null;
  }

  private function sqlGreatest(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;

    if (C\count($args) < 2) {
      throw new SQLFakeRuntimeException("MySQL GREATEST() function must be called with at two arguments");
    }

    $values = vec[];
    foreach ($this->args as $arg) {
      $val = $arg->evaluate($row, $conn);
      $values[] = $val;
    }

    return \max($values);
  }

  private function sqlNullif(row $row, AsyncMysqlConnection $conn): mixed {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 2) {
      throw new SQLFakeRuntimeException("MySQL NULLIF() function must be called with two arguments");
    }
    $left = $args[0]->evaluate($row, $conn);
    $right = $args[1]->evaluate($row, $conn);

    return ($left === $right) ? null : $left;
  }

  private function sqlFromUnixtime(row $row, AsyncMysqlConnection $conn): string {
    $row = $this->maybeUnrollGroupedDataset($row);

    $args = $this->args;
    if (C\count($args) !== 1) {
      throw new SQLFakeRuntimeException("MySQL FROM_UNIXTIME() SQLFake only implemented for 1 argument");
    }

    $column = $args[0]->evaluate($row, $conn);


    //
    // This is the default format from MySQL ‘YYYYY-MM-DD HH:MM:SS’
    //

    $format = 'Y-m-d G:i:s';
    return \date($format, (int)$column);
  }

  private function sqlConcat(row $row, AsyncMysqlConnection $conn): string {
    $row = $this->maybeUnrollGroupedDataset($row);
    $args = $this->args;
    if (C\count($args) < 2) {
      throw new SQLFakeRuntimeException("MySQL CONCAT() function must be called with at least two arguments");
    }
    $final_concat = "";
    foreach ($args as $k => $arg) {
      $val = (string)$arg->evaluate($row, $conn);
      $final_concat .= $val;
    }
    return $final_concat;
  }

  private function sqlConcatWS(row $row, AsyncMysqlConnection $conn): string {
    $row = $this->maybeUnrollGroupedDataset($row);
    $args = $this->args;
    if (C\count($args) < 2) {
      throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function must be called with at least two arguments");
    }
    $separator = $args[0]->evaluate($row, $conn);
    if ($separator === null) {
      throw new SQLFakeRuntimeException("MySQL CONCAT_WS() function required non null separator");
    }
    $separator = (string)$separator;
    $final_concat = "";
    foreach ($args as $k => $arg) {
      if ($k < 1) {
        continue;
      }
      $val = (string)$arg->evaluate($row, $conn);
      if (Str\is_empty($final_concat)) {
        $final_concat = $final_concat.$val;
      } else {
        $final_concat = $final_concat.$separator.$val;
      }
    }
    return $final_concat;
  }

  private function sqlField(row $row, AsyncMysqlConnection $conn): mixed {
    $args = $this->args;
    $num_args = C\count($args);
    if ($num_args < 2) {
      throw new SQLFakeRuntimeException("MySQL FIELD() function must be called with at least two arguments");
    }

    $value = $args[0]->evaluate($row, $conn);
    foreach ($args as $k => $arg) {
      if ($k < 1) {
        continue;
      }
      if ($value == $arg->evaluate($row, $conn)) {
        return $k;
      }
    }
    return 0;
  }

  private function sqlValues(row $row, AsyncMysqlConnection $conn): mixed {
    $args = $this->args;
    $num_args = C\count($args);
    if ($num_args !== 1) {
      throw new SQLFakeRuntimeException("MySQL VALUES() function must be called with one argument");
    }

    $arg = $args[0];
    if (!$arg is ColumnExpression) {
      throw new SQLFakeRuntimeException("MySQL VALUES() function should be called with a column name");
    }

    // a bit hacky here, override so that the expression pulls the value from the sql_fake_values.* fields set in Query::applySet
    $arg->prefixColumnExpression('sql_fake_values.');
    return $arg->evaluate($row, $conn);
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    $args = vec[];
    foreach ($this->args as $arg) {
      $args[] = \var_dump($arg, true);
    }
    return dict[
      'type' => (string)$this->type,
      'functionName' => $this->functionName,
      'args' => $args,
      'name' => $this->name,
      'distinct' => $this->distinct,
    ];
  }
}
