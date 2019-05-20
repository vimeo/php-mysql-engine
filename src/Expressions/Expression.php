<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

/*
* An ExpressionParser Expression is something that can be evaluated one row at a time
* Evaluating it will yield a constant value.
* They should only have to be built once during query planning,
* with references to column indexes in the query result set, so that
* no additional parsing of column names or aliases needs to be done to
* evaluate them on each row.
*
* This is an inherently recursive object structure. Besides the "constant" and "column"
* expression types, all others will have child elements which are
* themselves of type Expression. In the end they should all have columns or constants as
* inner-most Expression types. For example, a BinaryOperatorExpression
* for the operation "5+5" will have a ConstantExpression as its "left" and "right",
* each of which return 5 when evaluated.
*
*/
abstract class Expression {

  public string $operator = '';
  public bool $negated = false;
  public int $precedence;
  public string $name;
  protected TokenType $type;
  protected bool $evaluates_groups = false;

  /*
   * many expressions won't support negation,
   * and should throw a parse error if this is called
   * subclasses that do support negation must override this
   */
  public function negate(): void {
    throw new SQLFakeParseException("Parse error: unexpected NOT for expression {$this->type}");
  }

  /**
   * Expressions are built up incrementally when parsing
   * This function allows an expression to signify if it has all of the required sub-expressions,
   * such as having both the "left" and "right" operators for a binary expressions
   */
  public abstract function isWellFormed(): bool;

  /**
   * a lot of times you just want the value
   */
  public abstract function evaluate(row $row, AsyncMysqlConnection $conn): mixed;

  /**
   * when evaluating an expression in a Select, we want its name to use as the column name
   */
  public function evaluateWithName(row $row, AsyncMysqlConnection $conn): (string, mixed) {
    return tuple($this->name, $this->evaluate($row, $conn));
  }

  public function getType(): TokenType {
    return $this->type;
  }

  /**
   * Only some expression types support children
   * For example, set "right" on a binary, or "start" on a BETWEEN, or "when" on a case
   * For several of the types that don't have child elements this is just a parse error, children who implement it can override
   */
  public function setNextChild(Expression $_expr, bool $_overwrite = false): void {
    throw new SQLFakeParseException("Parse error: unexpected expression");
  }

  /**
   * Only some expression types support recursive expressions
   * otherwise if unimplemented, it's a parse error
   */
  public function addRecursiveExpression(token_list $_tokens, int $_pointer, bool $_negated = false): int {
    throw new SQLFakeParseException("Parse error: unexpected recursive expression");
  }

  /**
   * All operators have to handle potentially grouped data sets.
   * row is a dict<string, mixed>, but for a grouped data set the "mixed"
   * will itself be a dict<string, mixed>, so the row will be a dict<string, dict<string, mixed>>
   * See applyGroupBy which does the grouping
   * Since some operators don't want grouped data (column expressions and non-aggregate functions)
   * this helper lets them extract the first value from the grouping set
   */
  protected function maybeUnrollGroupedDataset(row $rows): row {
    $first = C\first($rows);
    if ($first is dict<_, _>) {
      /* HH_FIXME[4110] generics can't be specified here yet */
      return $first;
    }

    return $rows;
  }

  /**
   * Return a container-ish representation of an expression for pretty printing
   * used for logging and debugging.
   * Expressions with children SHOULD call this on all children too
   */
  public abstract function __debugInfo(): KeyedContainer<arraykey, mixed>;
}
