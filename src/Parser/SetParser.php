<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

// process the SET clause of an UPDATE, or the UPDATE portion of INSERT .. ON DUPLICATE KEY UPDATE
final class SetParser {

  public function __construct(private int $pointer, private token_list $tokens) {}


  public function parse(bool $skip_set = false): (int, vec<BinaryOperatorExpression>) {

    // if we got here, the first token had better be a SET
    if (!$skip_set && $this->tokens[$this->pointer]['value'] !== 'SET') {
      throw new DBMockParseException("Parser error: expected SET");
    }
    $expressions = vec[];
    $this->pointer++;
    $count = C\count($this->tokens);

    $needs_comma = false;
    $end_of_set = false;
    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      switch ($token['type']) {
        case TokenType::NUMERIC_CONSTANT:
        case TokenType::STRING_CONSTANT:
        case TokenType::OPERATOR:
        case TokenType::SQLFUNCTION:
        case TokenType::IDENTIFIER:
        case TokenType::PAREN:
          if ($needs_comma)
            throw new DBMockParseException("Expected , between expressions in SET clause");
          $expression_parser = new ExpressionParser($this->tokens, $this->pointer - 1);
          $start = $this->pointer;
          list($this->pointer, $expression) = $expression_parser->buildWithPointer();

          // the only valid kind of expression in a SET is "foo = bar"
          if (!$expression is BinaryOperatorExpression || $expression->operator !== '=') {
            throw new DBMockParseException("Failed parsing SET clause: unexpected expression");
          }

          if (!$expression->left is ColumnExpression)
            throw new DBMockParseException("Left side of SET clause must be a column reference");

          $expressions[] = $expression;
          $needs_comma = true;
          break;
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            if (!$needs_comma)
              throw new DBMockParseException("Unexpected ,");
            $needs_comma = false;
          } else {
            throw new DBMockParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::CLAUSE:
          // return once we get to the next clause
          $end_of_set = true;
          break;
        default:
          throw new DBMockParseException("Unexpected {$token['value']} in SET");
      }

      if ($end_of_set) break;

      $this->pointer++;
    }

    if (!C\count($expressions))
      throw new DBMockParseException("Empty SET clause");

    return tuple($this->pointer - 1, $expressions);
  }
}
