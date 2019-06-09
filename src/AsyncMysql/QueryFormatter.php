<?hh // strict

// Hack port of squangle/mysql_client/Query.cpp
// License below:
/*
 *  Copyright (c) 2016, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/**
 * The supported placeholders are:
 *
 * %T table name
 * %C column name
 * %s nullable string (will be escaped)
 * %d integer
 * %f float
 * %=s nullable string comparison - expands to either: = 'escaped_string' IS NULL
 * %=d nullable integer comparison
 * %=f nullable float comparison
 * %Q raw SQL query. The typechecker intentionally does not recognize this, however, you can use it in combination with // UNSAFE if absolutely required. Use this at your own risk as it could open you up for SQL injection.
 * %Lx where x is one of T, C, s, d, or f, represents a list of table names, column names, nullable strings, integers or floats, respectively. Pass a Vector of values to have it expanded into a comma-separated list. Parentheses are not added automatically around the placeholder in the query string, so be sure to add them if necessary.
 * With the exception of %Q, any strings provided will be properly escaped.
*/

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str};

/**
 * Format a query similar to AsyncMysqlConnection::queryf
 */
abstract final class QueryFormatter {
	public static function formatQuery(string $query, mixed ...$args): string {
		$sql = '';

		// string, size, offset
		// match types: d, f, v, s, m, u
		// digit, float, magic, string, m..., unsigned int?
		// the input accepts tuples and thruples for field references, could that be what "m" is? no

		$out = vec[];
		$query_length = Str\length($query);
		$after_percent = false;
		$args_pointer = 0;
		$args_count = C\count($args);
		for ($i = 0; $i < $query_length; $i++) {
			$c = $query[$i];
			if (!$after_percent) {
				if ($c !== '%') {
					$out[] = $c;
				} else {
					$after_percent = true;
				}
				continue;
			}

			$after_percent = false;
			if ($c === '%') {
				// %% escapes %
				$out[] = '%';
				continue;
			}

			if ($args_pointer === $args_count) {
				// we already parsed all arguments, but we found another %. parsing error
				throw new SQLFakeParseException('too few parameters for query: %'.$c.' has no param to bind');
			}

			$param = $args[$args_pointer++];
			switch ($c) {
				case 'd':
				case 's':
				case 'f':
				case 'u':
					$out = self::appendValue($out, $c, $param);
					break;
				case 'm':
					if (!($param is string || $param is num)) {
						throw new SQLFakeParseException("%m  expects int/float/string, ".\gettype($param).' found');
					}
					$out = self::appendValue($out, $c, $param);
					break;
				case 'K':
					$out[] = '/*';
					$out[] = Str\replace_every((string)$param, dict['/*' => '/ * ', '*/' => '* / ']);
					$out[] = '*/';
					break;
				case 'T':
				case 'C':
					$out = self::appendColumnTableName($out, $param);
					break;
				case '=':
					$type = $query[++$i];
					if (!C\contains_key(keyset['d', 's', 'f', 'u'], $type)) {
						throw new SQLFakeParseException("at %=$type, expected %=d, %=c, %=s, or %=u");
					}

					if ($param === null) {
						$out[] = ' IS NULL';
					} else {
						$out[] = ' = ';
						$out = self::appendValue($out, $type, $param);
					}
					break;
				case 'V':
					$row_len = 0;
					$first_row = true;
					$first_in_row = true;
					$param as Container<_>;
					foreach ($param as $row) {
						$first_in_row = true;
						$col_idx = 0;
						if (!$first_row) {
							$out[] = ", ";
						}
						$out[] = "(";
						$row as Container<_>;
						foreach ($row as $col) {
							if (!$first_in_row) {
								$out[] = ", ";
							}
							$out = self::appendValue($out, 'v', $col);
							$col_idx++;
							$first_in_row = false;
							if ($first_row) {
								$row_len++;
							}
						}
						$out[] = ")";
						if ($first_row) {
							$first_row = false;
						} else if ($col_idx != $row_len) {
							throw new SQLFakeParseException("not all rows provided for %V formatter are the same size");
						}
					}
					break;
				case 'L':
					$type = $query[++$i];
					if ($type === "O" || $type === "A") {
						$out[] = "(";
						$sep = ($type === "O") ? " OR " : " AND ";
						$out = self::appendValueClauses($out, $sep, $param);
						$out[] = ")";
					} else {
						if (!$param is Container<_>) {
							throw new SQLFakeParseException("expected array for %L formatter");
						}
						$first_param = true;
						foreach ($param as $val) {
							if (!$first_param) {
								$out[] = ", ";
							}
							$first_param = false;
							if ($type === "C") {
								$out = self::appendColumnTableName($out, $val);
							} else {
								$out = self::appendValue($out, $type[0], $val);
							}
						}
					}
					break;
				case 'U':
					$out = self::appendValueClauses($out, ', ', $param);
					break;
				case 'W':
					$out = self::appendValueClauses($out, ' AND ', $param);
					break;
				case 'Q':
					throw new SQLFakeNotImplementedException("%Q not supported in SQLFake");
					break;
				default:
					throw new SQLFakeParseException("unknown % code %$c");
			}
		}

		return Str\join($out, '');
	}

	private static function appendValue(vec<string> $out, string $type, mixed $param): vec<string> {
		if ($param is string) {
			if (!C\contains_key(keyset['s', 'v', 'm'], $type)) {
				throw new SQLFakeParseException("string value not valid for %$type");
			}
			// if this function is deprecated,
			// this could be changed to addslashes or really anything since we're NEVER passing these to an actual DB
			$out[] = '"'.\mysql_escape_string($param).'"';
		} else if ($param is int) {
			if (!C\contains_key(keyset['d', 'v', 'm', 'u'], $type)) {
				throw new SQLFakeParseException("int value not valid for %$type");
			}

			if ($type === 'u') {
				invariant($param >= 0, 'unsigned int must be nonnegative');
			}
			$out[] = (string)$param;
		} else if ($param is float) {
			if (!C\contains_key(keyset['f', 'v', 'm'], $type)) {
				throw new SQLFakeParseException("int value not valid for %$type");
			}
			$out[] = (string)$param;
		} else if ($param === null) {
			$out[] = 'NULL';
		} else {
			throw new SQLFakeParseException("Unexpected type for %$type: ".\gettype($param));
		}

		return $out;
	}

	private static function appendColumnTableName(vec<string> $out, mixed $param): vec<string> {
		if ($param is string) {
			$out[] = '`';
			$length = Str\length($param);
			for ($i = 0; $i < $length; $i++) {
				if ($param[$i] === '`') {
					$out[] = '`';
				}
				$out[] = $param[$i];
			}
			$out[] = '`';
		} elseif ($param is Container<_>) {
			switch (C\count($param)) {
				// qualified column name, possibly with alias
				case 2:
				case 3:
					$v = vec($param);
					$count = C\count($v);
					foreach ($v as $counter => $val) {
						$out = self::appendColumnTableName($out, $val);
						if ($counter === 0) {
							$out[] = '.';
						} elseif ($count > $counter + 1) {
							$out[] = ' AS ';
						}
					}
					break;
				default:
					throw new SQLFakeParseException("Unexpected container type for %T/%C");
			}
		}
		return $out;
	}

	private static function appendValueClauses(vec<string> $out, string $sep, mixed $param): vec<string> {
		if (!$param is KeyedContainer<_, _>) {
			throw new SQLFakeParseException("KeyedContainer expected for %Lx but received ".\gettype($param));
		}
		$first_param = true;
		foreach ($param as $key => $value) {
			if (!$first_param) {
				$out[] = $sep;
			}
			$first_param = false;
			$out = self::appendColumnTableName($out, $key);
			if ($value === null && $sep[0] !== ',') {
				$out[] = " IS NULL";
			} else {
				$out[] = " = ";
				$out = self::appendValue($out, 'v', $value);
			}
		}

		return $out;
	}
}
