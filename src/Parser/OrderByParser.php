<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

// parse the ORDER BY clause, which can be used for SELECT, UPDATE, or DELETE
final class OrderByParser {

  public function __construct(
    private int $pointer,
    private token_list $tokens,
    // this one is only used for SELECT queries.
    private ?vec<Expression> $selectExpressions = null,
  ) {}

  public function parse(): (int, order_by_clause) {

    // if we got here, the first token had better be ORDER
    if ($this->tokens[$this->pointer]['value'] !== 'ORDER') {
      throw new DBMockParseException("Parser error: expected ORDER");
    }

    $this->pointer++;
    $next = $this->tokens[$this->pointer] ?? null;
    $expressions = vec[];
    if ($next === null || $next['value'] !== 'BY')
      throw new DBMockParseException("Expected BY after ORDER");

    while (true) {
      $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
      if ($this->selectExpressions is nonnull) {
        $expression_parser->setSelectExpressions($this->selectExpressions);
      }
      list($this->pointer, $expression) = $expression_parser->buildWithPointer();

      // any constants in the ORDER BY must be positional references
      if ($expression is ConstantExpression) {
        // SELECT is evaluated before ORDER BY, so we can use a PositionExpression to grab the value
        $position = (int)($expression->value);

        $expression = new PositionExpression($position);
      }

      $next = $this->tokens[$this->pointer + 1] ?? null;

      // default to ASC
      $sort_direction = SortDirection::ASC;
      if ($next !== null && C\contains_key(keyset['ASC', 'DESC'], $next['value'])) {
        $this->pointer++;
        $sort_direction = SortDirection::assert($next['value']);
        $next = $this->tokens[$this->pointer + 1] ?? null;
      }

      $expressions[] = shape('expression' => $expression, 'direction' => $sort_direction);

      // skip over commas and continue the processing, but if it's any other token break out of the loop
      if ($next === null || $next['value'] !== ',') {
        break;
      }
      $this->pointer++;
    }

    return tuple($this->pointer, $expressions);
  }
}
