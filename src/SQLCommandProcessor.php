<?hh // strict

namespace Slack\DBMock;

/**
 * The query running interface
 * This parses a SQL statement using the Parser, then takes the parsed Query representation and executes it
 */
final class SQLCommandProcessor {

	# todo maybe move $query and $subquery_row to another function?
	public function execute(
		string $sql,
		AsyncMysqlConnection $conn,
		?SelectQuery $query = null,
		?row $subquery_row = null,
	): (dataset, int) {
		try {
			$query = SQLParser::parse($sql);
		} catch (\Exception $e) {
			// this makes debugging a failing unit test easier, show the actual query that failed parsing along with the parser error
			$msg = $e->getMessage();
			$type = \get_class($e);
			throw new DBMockParseException("DB Mock $type: $msg in SQL query: $sql");
		}

		if ($query is SelectQuery) {
			return tuple($query->execute($conn), 0);
		} elseif ($query is UpdateQuery) {
			return tuple(vec[], $query->execute($conn));
		} elseif ($query is DeleteQuery) {
			return tuple(vec[], $query->execute($conn));
		} elseif ($query is InsertQuery) {
			return tuple(vec[], $query->execute($conn));
		} else {
			// TODO handle SET, BEGIN, etc.
			return tuple(vec[], 0);
		}
	}
}
