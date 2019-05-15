<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\Str;

/**
 * just returns a constant value
 */
final class ConstantExpression extends Expression {

  public mixed $value;

  public function __construct(token $token) {
    $this->type = $token['type'];
    $this->precedence = 0;
    $this->name = $token['value'];
    $this->value = $this->extractConstantValue($token);
  }

  private function extractConstantValue(token $token): mixed {
    switch ($token['type']) {
      case TokenType::NUMERIC_CONSTANT:
        if (Str\contains((string)$token['value'], '.')) {
          return (float)$token['value'];
        }
        return (int)$token['value'];
      case TokenType::STRING_CONSTANT:
        return (string)$token['value'];
      case TokenType::NULL_CONSTANT:
        return null;
      default:
        throw
          new DBMockRuntimeException("Attempted to assign invalid token type {$token['type']} to Constant Expression");
    }
  }

  <<__Override>>
  public function evaluate(row $_row, AsyncMysqlConnection $_conn): mixed {
    return $this->value;
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return true;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, mixed> {
    return dict['type' => 'const', 'name' => $this->name, 'value' => $this->value];
  }
}
