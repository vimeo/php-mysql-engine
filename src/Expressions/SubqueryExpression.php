<?hh // strict

namespace Slack\DBMock;

/**
 * Contains an entier query you can run. this expression is the reason that every expression
 * takes a $database object, since the subquery can query another table in the database
 */
final class SubqueryExpression extends Expression {

	public function __construct(private SelectQuery $query, public string $name) {
		$this->precedence = 0;
		$this->type = TokenType::CLAUSE;
	}

	<<__Override>>
	public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
		return db_mock_query_select('', '', $database, $this->query, $row);
	}

	<<__Override>>
	public function isWellFormed(): bool {
		return true;
	}

	<<__Override>>
	public function __debugInfo(): dict<string, mixed> {
		return dict['type' => 'subquery', 'query' => $this->query, 'name' => $this->name];
	}
}
