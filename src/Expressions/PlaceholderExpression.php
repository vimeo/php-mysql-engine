<?hh // strict

namespace Slack\DBMock;

/**
 * the placeholder is often used before we know which operator will be used in some expression
 * it always has to get replaced by a real expression before parsing is over, and will throw if used
 * but this allows us to avoid having nullable expressions everywhere and constantly asserting they aren't null at runtime
 */
final class PlaceholderExpression extends Expression {

  public function __construct() {
    $this->precedence = 0;
    $this->name = '';
    $this->type = TokenType::RESERVED;
  }

  <<__Override>>
  public function evaluate(row $_row, AsyncMysqlConnection $_conn): mixed {
    throw new DBMockRuntimeException("Attempted to evaluate placeholder expression!");
  }

  <<__Override>>
  public function isWellFormed(): bool {
    return false;
  }

  <<__Override>>
  public function __debugInfo(): dict<string, string> {
    return dict['type' => 'placeholder'];
  }
}
