<?hh // strict

namespace Slack\DBMock;

final class DeleteQuery extends Query {
	public ?from_table $fromClause = null;

	public function __construct(public string $sql) {}
}
