<?hh // strict

namespace Slack\DBMock;

final class UpdateQuery extends Query {

	public function __construct(public from_table $update_clause, public string $sql) {}

	public vec<BinaryOperatorExpression> $setClause = vec[];
}
