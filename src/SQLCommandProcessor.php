<?hh // strict

namespace Slack\DBMock;

use namespace HH\Lib\C;

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
			throw new DBMockParseException(
				"DB Mock $type: $msg in SQL query: $sql",
			);
		}

		if ($query is SelectQuery) {
			return tuple($query->execute($conn), 0);
		} elseif ($query is UpdateQuery) {

		} elseif ($query is DeleteQuery) {

		} elseif ($query is InsertQuery) {
			return tuple(vec[], $query->execute($conn));
		}

		# also handle SET/BEGIN/etc.?

		# actually why not just have the parsed query contain an execute function which takes a $conn?
		# have any functions used by multiple query types live on Query
		# and the ones specific to a single type live in that type
	}

}

/**
* This file is the interface to the outside world. Pass it a string of SQL and a DB, and it does the rest.
*/

/**
* Process and run a SELECT statement against the given database, which contains all the table data keyed by name
* Only the first elements are specified for calls into this library, but for recursive calls with subqueries the latter two may be specified
* subquery_row is currently unused, but is there for possibly extending to support correlated subqueries
*/
function db_mock_query_select(
	string $sql,
	AsyncMysqlConnection $conn,
	?SelectQuery $query = null,
	?row $subquery_row = null,
): dataset {
	// parsed is passed in for subqueries in the select/where clause, since parser parses subqueries along with main query
	if ($query === null) {
		try {
			$query = SQLParser::parse($sql);
		} catch (\Exception $e) {
			// this makes debugging a failing unit test easier, show the actual query that failed parsing along with the parser error
			$msg = $e->getMessage();
			$type = \get_class($e);
			throw new DBMockParseException(
				"DB Mock $type: $msg in SQL query: $sql",
			);
		}
	}

	if (!$query is SelectQuery) {
		throw new DBMockParseException(
			"Called db_mock_query_select but the query was not a SELECT: $sql",
		);
	}

	// FROM clause handling - builds a data set including extracting rows from tables, applying joins. also builds an index of all the columns present in that dataset which is used in all other clauses
	$data = db_mock_query_process_from($conn, $query, $sql);

	// WHERE caluse - filter out any rows that don't match it
	$data = db_mock_query_apply_where($conn, $data, $query);

	// GROUP BY clause - may group the rows if necessary. all expressions after this need to know how to handled grouped and ungrouped inputs
	$data = db_mock_query_apply_group_by($conn, $data, $query);

	// HAVING clause, filter out any rows not matching it
	$data = db_mock_query_apply_having($conn, $data, $query);

	// SELECT clause. this is where we actually run the expressions in the SELECT
	$data = db_mock_query_apply_select($conn, $data, $query);

	// ORDER BY. this runs after select because it could use expressions from the select
	$data = db_mock_apply_order_by($conn, $data, $query);

	// LIMIT clause
	$data = db_mock_apply_limit($data, $query);

	// filter out any data that we needed for the ORDER BY that is not supposed to be returned
	$data = db_mock_query_order_by_filtering($conn, $data, $query);

	// this recurses in case there are any UNION, EXCEPT, INTERSECT keywords
	return db_mock_process_multi_query($conn, $data, $query);
}

/**
* Handle a _db_write_raw call, which could be an UPDATE or DELETE
* TODO

function db_mock_write(string $sql, AsyncMysqlConnection $conn): int {
	try {
		$query = SQLParser::parse($sql);
	} catch (\Exception $e) {
		// this makes debugging a failing unit test easier, show the actual query that failed parsing along with the parser error
		$msg = $e->getMessage();
		throw new DBMockParseException("DB Mock parse error: $msg in SQL query: $sql");
	}

	if ($query is UpdateQuery) {
		return db_mock_query_update($cluster, $query, $conn, $sql);
	} elseif ($query is DeleteQuery) {
		return db_mock_query_delete($cluster, $query, $conn, $sql);
	} else {
		throw new DBMockParseException("Unimplemented statement type in DB mock _db_write_raw: $sql");
	}
}
*/

/**
* this is used for _db_mock_db_insert_dupe_manual_safe
*/
function db_mock_query_parse_set_fragment(
	string $sql,
): vec<BinaryOperatorExpression> {
	return SQLParser::parseSetFragment($sql);
}


/**
* TODO change to accept a string and do the parsing

function db_mock_query_update(UpdateQuery $query, AsyncMysqlConnection $conn, string $sql): int {

	list($table_name, $data) = db_mock_query_process_update($conn, $query);

	db_mock_info('('.$cluster.' -> '.$table_name.')');

	State::track_query(QueryType::UPDATE, $cluster, $table_name, $sql);

	QueryContext::set_have_schema(!is_null(db_tables_get_schema(_db_mock_get_cluster_type($cluster), $table_name)));

	$data = db_mock_query_apply_where($conn, $data, $query);

	$data = db_mock_apply_order_by($conn, $data, $query);

	$data = db_mock_apply_limit($data, $query);

	$table_schema = db_tables_get_schema(_db_mock_get_cluster_type($cluster), $table_name);

	return db_mock_query_apply_set($table_name, $conn, $data, $query->set_clause, $table_schema);
}
*/


/**
* Invoked by db_mock_write if the query is an update
* TODO change to accept a string and do the parsing

function db_mock_query_delete(DeleteQuery $query, AsyncMysqlConnection $conn, string $sql): int {

	list($table_name, $data) = db_mock_query_process_delete_from($conn, $query);

	db_mock_info('('.$cluster.' -> '.$table_name.')');

	State::track_query(QueryType::DELETE, $cluster, $table_name, $sql);

	QueryContext::set_have_schema(!is_null(db_tables_get_schema(_db_mock_get_cluster_type($cluster), $table_name)));

	$data = db_mock_query_apply_where($conn, $data, $query);

	$data = db_mock_apply_order_by($conn, $data, $query);

	$data = db_mock_apply_limit($data, $query);

	return db_mock_query_delete_rows($table_name, $conn, $data);
}
*/


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// UPDATE helpers
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
* process the UPDATE clause to retrieve the table
* add a row identifier to each element in the result which we can later use to update the underlying table
*/
function db_mock_query_process_update(
	AsyncMysqlConnection $conn,
	UpdateQuery $query,
): (string, dataset) {
	$table_name = $query->update_clause['name'];

	$res = idx($conn, $table_name);

	if ($res === null) {
		$res = Vector {};
		// it seems very common for code not to create tables before they might be updated. in this case, let's just return an empty table for now
		// some day it would be nicer to use shapes to create the 'schema' when db mock is initialized so that this is more validated
		//throw new DBMockParseException("Unable to process query for unknown table {$table['table']}");
	}

	$data = Vector {};
	foreach ($res as $i => $row) {
		$m = new Map($row);
		// row identifier used for updating the data
		$m->set('db_mock_row_id', $i);
		$data[] = $m;
	}

	return tuple($table_name, $data);
}


/**
* unlike most other functions this takes its clause on its own instead of as a query
* this is because it can be invoked from outside for _db_mock_db_insert_dupe_manual_safe
*/
function db_mock_query_apply_set(
	string $table_name,
	AsyncMysqlConnection $conn,
	dataset $data,
	Vector<BinaryOperatorExpression> $set_clause,
	?db_table_schema_t $table_schema,
): int {

	$table = $conn->get($table_name);
	if ($table === null)
		return 0;

	$valid_fields = null;
	if ($table_schema !== null) {
		$valid_fields = keyset[];
		foreach ($table_schema['fields'] as $field) {
			$valid_fields[] = $field['name'];
		}
	}

	$set_clauses = Vector {};
	foreach ($set_clause as $expression) {
		$left = $expression->left;
		// the parser already asserts this at parse time
		assert($left is ColumnExpression);
		$column = $left->name;

		$right = $expression->right;
		assert($right !== null);

		// If we know the valid fields for this table, only allow setting those
		if ($valid_fields !== null) {
			if (!C\contains($valid_fields, $column)) {
				throw new DBMockParseException(
					"Invalid update column {$table_name}.{$column}",
				);
			}
		}

		$set_clauses[] = shape('column' => $column, 'expression' => $right);
	}

	$update_count = 0;

	foreach ($data as $row) {
		$changes_found = false;
		$row_id = $row->get('db_mock_row_id');
		$row->removeKey('db_mock_row_id');

		// explicitly make an array keyed by strings to appease type checker
		$new_row = array();
		foreach ($row as $col => $val)
			$new_row[(string)$col] = $val;

		assert($row_id is int);
		foreach ($set_clauses as $clause) {
			$existing_value = $new_row[(string)$clause['column']];
			$new_value = $clause['expression']->evaluate($row, $conn, $server);

			if ($new_value !== $existing_value) {
				$new_row[(string)$clause['column']] = $new_value;
				$changes_found = true;
			}
		}

		if ($changes_found) {
			db_mock_info(
				'Update '.json_encode($row).' to '.json_encode($new_row),
			);
			$table->set($row_id, $new_row);
			$update_count++;
		}
	}
	return $update_count;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DELETE helpers
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/**
* after the rows to delete have been identified, this deletes them from the database (which is a reference implicitly)
*/
function db_mock_query_delete_rows(
	string $table_name,
	AsyncMysqlConnection $conn,
	dataset $data,
): int {

	$table = $conn->get($table_name);
	if ($table === null)
		return 0;

	$rows_to_delete = array();
	foreach ($data as $row) {
		db_mock_info('Delete '.json_encode($row));
		$row_id = $row->get('db_mock_row_id');
		assert($row_id is int);
		$rows_to_delete[] = $row_id;
	}

	// sort the ids descending, important because deleting a row from a vector re-keys the vector and changes all later keys, so by doing this we don't have to care about that
	rsort(&$rows_to_delete);

	foreach ($rows_to_delete as $id) {
		$table->removeKey($id);
	}

	return count($rows_to_delete);
}

/**
* process the DELETE clause to retrieve the table
* add a row identifier to each element in the result which we can later use to delete from the underlying table
*/
function db_mock_query_process_delete_from(
	AsyncMysqlConnection $conn,
	DeleteQuery $query,
): (string, dataset) {
	assert(!is_null($query->fromClause));
	$table_name = $query->fromClause['name'];

	$res = idx($conn, $table_name);

	if ($res === null) {
		$res = Vector {};
		// it seems very common for code not to create tables before they might be updated. in this case, let's just return an empty table for now
		// some day it would be nicer to use shapes to create the 'schema' when db mock is initialized so that this is more validated
		//throw new DBMockParseException("Unable to process query for unknown table {$table['table']}");
	}

	$data = Vector {};
	foreach ($res as $i => $row) {
		$m = new Map($row);
		// row identifier used for updating the data
		$m->set('db_mock_row_id', $i);
		$data[] = $m;
	}

	return tuple($table_name, $data);
}
