<?hh // strict

namespace Slack\SQLFake;

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
  /**
   * Evaluate the subquery, passing the current row from the outer query along
   * for correlated subqueries (not currently supported)
   */
  public function evaluate(row $row, AsyncMysqlConnection $conn): dataset {
    return $this->query->execute($conn, $row);
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
