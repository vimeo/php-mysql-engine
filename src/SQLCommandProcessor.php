<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\Str;

/**
 * The query running interface
 * This parses a SQL statement using the Parser, then takes the parsed Query representation and executes it
 */
abstract final class SQLCommandProcessor {

  public static function execute(string $sql, AsyncMysqlConnection $conn): (dataset, int) {

    // Check for unsupported statements
    if (Str\starts_with_ci($sql, 'SET') || Str\starts_with_ci($sql, 'BEGIN') || Str\starts_with_ci($sql, 'COMMIT')) {
      // we don't do any handling for these kinds of statements currently
      return tuple(vec[], 0);
    }

    if (Str\starts_with_ci($sql, 'ROLLBACK')) {
      // unlike BEGIN and COMMIT, this actually needs to have material effect on the observed behavior
      // even in a single test case, and so we need to throw since it's not implemented yet
      // there's no reason we couldn't start supporting transactions in the future, just haven't done the work yet
      throw new SQLFakeNotImplementedException("Transactions are not yet supported");
    }

    $query = SQLParser::parse($sql);

    if ($query is SelectQuery) {
      return tuple($query->execute($conn), 0);
    } elseif ($query is UpdateQuery) {
      return tuple(vec[], $query->execute($conn));
    } elseif ($query is DeleteQuery) {
      return tuple(vec[], $query->execute($conn));
    } elseif ($query is InsertQuery) {
      return tuple(vec[], $query->execute($conn));
    } else {
      throw new SQLFakeNotImplementedException("Unhandled query type: ".\get_class($query));
    }
  }
}
