<?hh // strict

namespace Slack\DBMock;

/**
 * a row expression is like (col1, col2, col3) and is sometimes used for a row comparison for pagination
 * such as: (col1, col2, col3) > (1, 2, 3)
 */
final class RowExpression extends Expression {

	public function __construct(private vec<Expression> $elements) {
		$this->precedence = 0;
		$this->name = '';
		$this->type = TokenType::PAREN;
	}

	<<__Override>>
	public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {

		$result = vec[];

		foreach ($this->elements as $expr) {
			$result[] = $expr->evaluate($row, $conn);
		}
		return $result;
	}

	<<__Override>>
	public function isWellFormed(): bool {
		return true;
	}

	<<__Override>>
	public function __debugInfo(): dict<string, mixed> {
		$elements = vec[];
		foreach ($this->elements as $elem) {
			$elements[] = \var_dump($elem, true);
		}
		return dict['type' => 'row_expression', 'name' => $this->name, 'elements' => $elements];
	}
}
