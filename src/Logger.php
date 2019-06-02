<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset, Math, Str};
use namespace HH\Lib\Experimental\IO;

abstract final class Logger {

	protected static ?IO\WriteHandle $handle = null;

	public static function setHandle(IO\WriteHandle $handle): void {
		self::$handle = $handle;
	}

	public static function log(Verbosity $verbosity, string $message): void {
		if (QueryContext::$verbosity >= $verbosity) {
			self::write("\n".$message);
		}
	}

	protected static function write(string $message): void {
		if (self::$handle is nonnull) {
			\HH\Asio\join(self::$handle->writeAsync($message));
		} else {
			\error_log($message);
		}
	}

	/**
	 * Log a query result with tabular formatting
	 *
	 * This function creates a formatted table view similar to the default view of the MySQL
	 * command line client.
	 *
	 * Example result:
	 *
	 *   +------+----------------------+-----------+
	 *   | id   | email                | username  |
	 *   +------+----------------------+-----------+
	 *   | 1234 | user1@slack-corp.com | username1 |
	 *   +------+----------------------+-----------+
	 *   1 row from cluster1
	 *
	 */
	public static function logResult(string $server, dataset $data, int $rows_affected): void {
		if (QueryContext::$verbosity >= Verbosity::RESULTS) {
			if ($rows_affected > 0) {
				self::write("{$rows_affected} rows affected\n");
				return;
			} elseif (!$data) {
				self::write("No results\n");
				return;
			}
			self::write(static::formatData($data, $server));
		}
	}

	private static function formatData(dataset $rows, string $server): string {
		$count = C\count($rows);

		$tbl_columns = static::formatColumns($rows);
		$all_columns = Keyset\keys($tbl_columns);
		$separator = static::formatTableSeparator($tbl_columns);

		$out = "\n";
		$out .= $separator;
		$out .= static::formatTableRow($tbl_columns, Dict\associate($all_columns, $all_columns));
		$out .= $separator;

		foreach ($rows as $row) {
			$out .= static::formatTableRow($tbl_columns, $row);
		}
		$out .= $separator;
		$out .= "$count row".($count === 1 ? '' : 's').' in set';

		$out .= " from ".$server;
		$out .= "\n";

		return $out;
	}

	/**
	 * Determine maximum string length of column names or values
	 */
	protected static function formatColumns(dataset $data): dict<string, int> {

		$columns = dict[];
		foreach ($data as $row) {
			foreach ($row as $col => $val) {
				if (C\contains_key($columns, $col)) {
					$columns[$col] = Math\maxva(Str\length($col), Str\length((string)$val));
				} else {
					$columns[$col] = Str\length((string)$val);
				}
			}
		}

		return $columns;
	}

	/**
	 * This function creates a horizontal separator line based on the maximum length
	 * of columns/column-values.
	 *
	 * Example result:
	 *
	 *   +------+----------------------+-----------+
	 */
	protected static function formatTableSeparator(dict<string, int> $columns): string {
		$out = '';
		foreach ($columns as $len) {
			$out .= '+'.Str\repeat('-', $len + 2);
		}
		$out .= "+\n";
		return $out;
	}

	/**
	 * generate a horizontal table row with separators
	 *
	 * This function creates a horizontal table row, given a map of column names and
	 * maximum string lengths, it uses these lengths to determine the padding for each
	 * column value in $row and joins each column with vertical separators.
	 *
	 * Example result:
	 *
	 *   | 1234 | user1@slack-corp.com | username1 |
	 */
	protected static function formatTableRow(dict<string, int> $columns, row $row): string {
		$out = '';
		foreach ($columns as $col => $len) {
			if (C\contains_key($row, $col)) {
				$out .= '| '.Str\pad_right((string)$row[$col], $len + 1);
			} else {
				$out .= '| '.Str\pad_right('NULL', $len + 1);
			}
		}
		$out .= "|\n";

		return $out;
	}

}
