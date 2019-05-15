<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\{C, Keyset, Vec};

final class DeleteQuery extends Query {
	public ?from_table $fromClause = null;

	public function __construct(public string $sql) {}

	public function execute(AsyncMysqlConnection $conn): int {
		$this->fromClause as nonnull;
		list($database, $table_name) = Query::parseTableName($conn, $this->fromClause['name']);
		$data = $conn->getServer()->getTable($database, $table_name) ?? vec[];
		Metrics::trackQuery(QueryType::DELETE, $conn->getServer()->name, $table_name, $this->sql);

		return $this->applyWhere($conn, $data)
			|> $this->applyOrderBy($conn, $$)
			|> $this->applyLimit($$)
			|> $this->applyDelete($conn, $database, $table_name, $$, $data);
	}

	/**
	 * Delete rows after all filtering clauses, and return the number of rows deleted
	 */
	protected function applyDelete(
		AsyncMysqlConnection $conn,
		string $database,
		string $table_name,
		dataset $filtered_rows,
		dataset $original_table,
	): int {

		// if this isn't a dict keyed by the original ids in the row, it could delete the wrong rows
		$filtered_rows as dict<_, _>;

		$rows_to_delete = Keyset\keys($filtered_rows);
		$remaining_rows =
			Vec\filter_with_key($original_table, ($row_num, $_) ==> !C\contains_key($rows_to_delete, $row_num));
		$rows_affected = C\count($remaining_rows) - C\count($original_table);

		// write it back to the database
		$conn->getServer()->saveTable($database, $table_name, $remaining_rows);
		return $rows_affected;
	}
}
