<?hh // strict

/**
 * Ported to Hack from https://github.com/iamcal/SQLParser
 *
 * MIT License
 *
 * Copyright (c) 2013-2017 Cal Henderson
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
*/

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Regex, Str, Vec};

type parsed_table = shape(
	'name' => string,
	'fields' => vec<parsed_field>,
	'sql' => string,
	'indexes' => vec<parsed_index>,
	'props' => dict<string, string>,
);

type parsed_field = shape(
	'name' => string,
	'type' => string,
	?'length' => string,
	?'unsigned' => bool,
	?'null' => bool,
	?'default' => string,
	...
);

type parsed_index = shape(
	?'name' => string,
	'type' => string,
	'cols' => vec<
		shape(
			'name' => string,
			?'length' => int,
			?'direction' => string,
		)
	>,
	?'mode' => string,
	?'parser' => string,
	?'more' => mixed,
	?'key_block_size' => string,
);

final class CreateTableParser {

	#
	# the main public interface is very simple
	#

	public function parse(string $sql): dict<string, parsed_table> {
		// stashes tokens and source_map in $this
		$this->lex($sql);
		return $this->walk($this->tokens, $sql, $this->sourceMap);
	}

	#
	# everything below here is private
	#

	private vec<string> $tokens = vec[];
	private vec<(int, int)> $sourceMap = vec[];

	#
	# lex and collapse tokens
	#
	private function lex(string $sql): vec<string> {
		$this->sourceMap = $this->lexImpl($sql);
		$this->tokens = $this->extractTokens($sql, $this->sourceMap);
		return $this->tokens;
	}

	#
	# simple lexer based on http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt
	#
	# returns a vec of [position, len] tuples for each token

	private function lexImpl(string $sql): vec<(int, int)> {

		$pos = 0;
		$len = Str\length($sql);

		$source_map = vec[];

		while ($pos < $len) {

			# <space>
			# <newline>

			$match = Regex\first_match($sql, re"!\s+!A", $pos);
			if ($match is nonnull) {
				$pos += Str\length($match[0]);
				continue;
			}

			# <comment>
			if (Regex\matches($sql, re"!--!A", $pos)) {
				$p2 = Str\search($sql, "\n", $pos);
				if ($p2 === null) {
					$pos = $len;
				} else {
					$pos = $p2 + 1;
				}
				continue;
			}

			if (Regex\matches($sql, re"!\\*!A", $pos)) {
				$p2 = Str\search($sql, "*/", $pos);
				if ($p2 === null) {
					$pos = $len;
				} else {
					$pos = $p2 + 2;
				}
				continue;
			}


			# <regular identifier>
			# <key word>g
			$match = Regex\first_match($sql, re"![[:alpha:]][[:alnum:]_]*!A", $pos);
			if ($match is nonnull) {
				$source_map[] = tuple($pos, Str\length($match[0] ?? ''));
				$pos += Str\length($match[0]);
				continue;
			}

			# backtick quoted field
			if (Str\slice($sql, $pos, 1) === '`') {
				$p2 = Str\search($sql, "`", $pos + 1);
				if ($p2 === null) {
					$pos = $len;
				} else {
					$source_map[] = tuple($pos, 1 + $p2 - $pos);
					$pos = $p2 + 1;
				}
				continue;
			}

			# <unsigned numeric literal>
			#	<unsigned integer> [ <period> [ <unsigned integer> ] ]
			#	<period> <unsigned integer>
			#	<unsigned integer> ::= <digit>...
			$match = Regex\first_match($sql, re"!(\d+\.?\d*|\.\d+)!A", $pos);
			if ($match is nonnull) {
				$source_map[] = tuple($pos, Str\length($match[0]));
				$pos += Str\length($match[0]);
				continue;
			}

			# <approximate numeric literal> :: <mantissa> E <exponent>
			# <national character string literal>
			# <bit string literal>
			# <hex string literal>

			# <character string literal>
			if ($sql[$pos] === "'" || $sql[$pos] === '"') {
				$c = $pos + 1;
				$q = $sql[$pos];
				while ($c < Str\length($sql)) {
					if ($sql[$c] === '\\') {
						$c += 2;
						continue;
					}
					if ($sql[$c] === $q) {
						$slen = $c + 1 - $pos;
						$source_map[] = tuple($pos, $slen);
						$pos += $slen;
						break;
					}
					$c++;
				}
				continue;
			}

			# <date string>
			# <time string>
			# <timestamp string>
			# <interval string>
			# <delimited identifier>
			# <SQL special character>
			# <not equals operator>
			# <greater than or equals operator>
			# <less than or equals operator>
			# <concatenation operator>
			# <double period>
			# <left bracket>
			# <right bracket>
			$source_map[] = tuple($pos, 1);
			$pos++;
		}

		return $source_map;
	}


	private function walk(vec<string> $tokens, string $sql, vec<(int, int)> $source_map): dict<string, parsed_table> {


		#
		# split into statements
		#

		$statements = vec[];
		$temp = vec[];
		$start = 0;
		for ($i = 0; $i < C\count($tokens); $i++) {
			$t = $tokens[$i];
			if ($t === ';') {
				if (C\count($temp)) {
					$statements[] = shape(
						"tuples" => $temp,
						"sql" => Str\slice(
							$sql,
							$source_map[$start][0],
							$source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1],
						),
					);
				}
				$temp = vec[];
				$start = $i + 1;
			} else {
				$temp[] = $t;
			}
		}
		if (C\count($temp)) {
			$statements[] = shape(
				"tuples" => $temp,
				"sql" => Str\slice(
					$sql,
					$source_map[$start][0],
					$source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1],
				),
			);
		}

		#
		# find CREATE TABLE statements
		#

		$tables = dict[];

		foreach ($statements as $stmt) {
			$s = $stmt['tuples'];

			if (Str\uppercase($s[0]) === 'CREATE TABLE') {

				$s = Vec\drop($s, 1);

				$table = $this->parseCreateTable($s, $stmt['sql']);
				$tables[$table['name']] = $table;
			}

			if (Str\uppercase($s[0]) === 'CREATE TEMPORARY TABLE') {

				$s = Vec\drop($s, 1);

				$table = $this->parseCreateTable($s, $stmt['sql']);
				$table['props']['temp'] = '1';
				$tables[$table['name']] = $table;
			}
		}

		return $tables;
	}


	private function parseCreateTable(vec<string> $tokens, string $sql): parsed_table {

		if ($tokens[0] === 'IF NOT EXISTS') {
			$tokens = Vec\drop($tokens, 1);
		}


		#
		# name
		#

		$t = $this->vecUnshift(inout $tokens);
		$name = $this->decodeIdentifier($t);


		#
		# CREATE TABLE x LIKE y
		#

		if ($this->nextTokenIs($tokens, 'LIKE')) {
			$this->vecUnshift(inout $tokens);
			$t = $this->vecUnshift(inout $tokens);
			$old_name = $this->decodeIdentifier($t);

			return shape(
				'name' => $name,
				'sql' => $sql,
				'props' => dict['like' => $old_name],
				'fields' => vec[],
				'indexes' => vec[],
			);
		}


		#
		# create_definition
		#

		$fields = vec[];
		$indexes = vec[];

		if ($this->nextTokenIs($tokens, '(')) {
			$tokens = Vec\drop($tokens, 1);
			$ret = $this->parseCreateDefinition(inout $tokens);
			$fields = $ret['fields'];
			$indexes = $ret['indexes'];
		}

		$props = $this->parseTableProps(inout $tokens);

		$table = shape(
			'name' => $name,
			'fields' => $fields,
			'indexes' => $indexes,
			'props' => $props,
			'sql' => $sql,
		);

		return $table;
	}


	private function nextTokenIs(vec<string> $tokens, string $val): bool {
		return Str\uppercase($tokens[0]) === $val;
	}

	private function parseCreateDefinition(
		inout vec<string> $tokens,
	): shape(
		'fields' => vec<parsed_field>,
		'indexes' => vec<parsed_index>,
	) {

		$fields = vec[];
		$indexes = vec[];

		while ($tokens[0] !== ')') {

			$these_tokens = $this->sliceUntilNextField(inout $tokens);

			$this->parseFieldOrKey(inout $these_tokens, inout $fields, inout $indexes);
		}

		$tokens = Vec\drop($tokens, 1); # closing paren

		return shape(
			'fields' => $fields,
			'indexes' => $indexes,
		);
	}

	private function parseFieldOrKey(
		inout vec<string> $tokens,
		inout vec<parsed_field> $fields,
		inout vec<parsed_index> $indexes,
	): void {

		#
		# parse a single create_definition
		#

		$has_constraint = false;
		$constraint = null;


		#
		# constraints can come before a few different things
		#

		if ($tokens[0] === 'CONSTRAINT') {

			$has_constraint = true;

			if (
				$tokens[1] === 'PRIMARY KEY' ||
				$tokens[1] === 'UNIQUE' ||
				$tokens[1] === 'UNIQUE KEY' ||
				$tokens[1] === 'UNIQUE INDEX' ||
				$tokens[1] === 'FOREIGN KEY'
			) {
				$tokens = Vec\drop($tokens, 1);
			} else {
				$tokens = Vec\drop($tokens, 1);
				$constraint = $this->vecUnshift(inout $tokens);
			}
		}


		switch ($tokens[0]) {

			#
			# named indexes
			#
			# INDEX		[index_name]	[index_type] (index_col_name,...) [index_option] ...
			# KEY		[index_name]	[index_type] (index_col_name,...) [index_option] ...
			# UNIQUE	[index_name]	[index_type] (index_col_name,...) [index_option] ...
			# UNIQUE INDEX	[index_name]	[index_type] (index_col_name,...) [index_option] ...
			# UNIQUE KEY	[index_name]	[index_type] (index_col_name,...) [index_option] ...
			#

			case 'INDEX':
			case 'KEY':
			case 'UNIQUE':
			case 'UNIQUE INDEX':
			case 'UNIQUE KEY':

				$index = shape(
					'type' => 'INDEX',
					'cols' => vec[],
				);

				if (C\contains_key(keyset['UNIQUE', 'UNIQUE INDEX', 'UNIQUE KEY'], $tokens[0])) {
					$index['type'] = 'UNIQUE';
				}

				$tokens = Vec\drop($tokens, 1);

				if ($tokens[0] !== '(' && $tokens[0] !== 'USING BTREE' && $tokens[0] !== 'USING HASH') {
					$t = $this->vecUnshift(inout $tokens);
					$index['name'] = $this->decodeIdentifier($t);
				}

				$this->parseIndexType(inout $tokens, inout $index);
				$this->parseIndexColumns(inout $tokens, inout $index);
				$this->parseIndexOptions(inout $tokens, inout $index);


				if (C\count($tokens)) {
					$index['more'] = $tokens;
				}
				$indexes[] = $index;
				return;


			#
			# PRIMARY KEY [index_type] (index_col_name,...) [index_option] ...
			#

			case 'PRIMARY KEY':

				$index = shape(
					'type' => 'PRIMARY',
					'cols' => vec[],
				);

				$tokens = Vec\drop($tokens, 1);

				$this->parseIndexType(inout $tokens, inout $index);
				$this->parseIndexColumns(inout $tokens, inout $index);
				$this->parseIndexOptions(inout $tokens, inout $index);

				if (C\count($tokens)) {
					$index['more'] = $tokens;
				}
				$indexes[] = $index;
				return;


			# FULLTEXT		[index_name] (index_col_name,...) [index_option] ...
			# FULLTEXT INDEX	[index_name] (index_col_name,...) [index_option] ...
			# FULLTEXT KEY		[index_name] (index_col_name,...) [index_option] ...
			# SPATIAL		[index_name] (index_col_name,...) [index_option] ...
			# SPATIAL INDEX		[index_name] (index_col_name,...) [index_option] ...
			# SPATIAL KEY		[index_name] (index_col_name,...) [index_option] ...

			case 'FULLTEXT':
			case 'FULLTEXT INDEX':
			case 'FULLTEXT KEY':
			case 'SPATIAL':
			case 'SPATIAL INDEX':
			case 'SPATIAL KEY':

				$index = shape(
					'type' => 'FULLTEXT',
					'cols' => vec[],
				);

				if (C\contains_key(keyset['SPATIAL', 'SPATIAL INDEX', 'SPATIAL KEY'], $tokens[0])) {
					$index['type'] = 'SPATIAL';
				}

				$tokens = Vec\drop($tokens, 1);

				if ($tokens[0] !== '(') {
					$t = $this->vecUnshift(inout $tokens);
					$index['name'] = $this->decodeIdentifier($t);
				}

				$this->parseIndexType(inout $tokens, inout $index);
				$this->parseIndexColumns(inout $tokens, inout $index);
				$this->parseIndexOptions(inout $tokens, inout $index);

				if (C\count($tokens)) {
					$index['more'] = $tokens;
				}
				$indexes[] = $index;
				return;


			# older stuff

			case 'CHECK':

				# not currently handled
				return;
		}

		$fields[] = $this->parseField($tokens);
	}

	private function sliceUntilNextField(inout vec<string> $tokens): vec<string> {

		$out = vec[];
		$stack = 0;

		while (C\count($tokens)) {
			$next = $tokens[0];
			if ($next === '(') {
				$stack++;
				$t = $this->vecUnshift(inout $tokens);
				$out[] = $t;
			} elseif ($next === ')') {
				if ($stack) {
					$stack--;
					$t = $this->vecUnshift(inout $tokens);
					$out[] = $t;
				} else {
					return $out;
				}
			} elseif ($next === ',') {
				if ($stack) {
					$t = $this->vecUnshift(inout $tokens);
					$out[] = $t;
				} else {
					$tokens = Vec\drop($tokens, 1);
					return $out;
				}
			} else {
				$t = $this->vecUnshift(inout $tokens);
				$out[] = $t;
			}
		}

		return $out;
	}

	private function parseField(vec<string> $tokens): parsed_field {

		$t = $this->vecUnshift(inout $tokens);
		$t2 = $this->vecUnshift(inout $tokens);
		$f = shape(
			'name' => $this->decodeIdentifier($t),
			'type' => Str\uppercase($t2),
		);

		switch ($f['type']) {

			# DATE
			case 'DATE':
			case 'TIME':
			case 'TIMESTAMP':
			case 'DATETIME':
			case 'YEAR':
			case 'TINYBLOB':
			case 'BLOB':
			case 'MEDIUMBLOB':
			case 'LONGBLOB':

				# nothing more to read
				break;

			# TINYINT[(length)] [UNSIGNED] [ZEROFILL]
			case 'TINYINT':
			case 'SMALLINT':
			case 'MEDIUMINT':
			case 'INT':
			case 'INTEGER':
			case 'BIGINT':

				$this->parseFieldLength(inout $tokens, inout $f);
				$this->parseFieldUnsigned(inout $tokens, inout $f);
				$this->parseFieldZerofill(inout $tokens, inout $f);
				break;


			# REAL[(length,decimals)] [UNSIGNED] [ZEROFILL]
			case 'REAL':
			case 'DOUBLE':
			case 'FLOAT':

				$this->parseFieldLengthDecimals(inout $tokens, inout $f);
				$this->parseFieldUnsigned(inout $tokens, inout $f);
				$this->parseFieldZerofill(inout $tokens, inout $f);
				break;


			# DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL]
			case 'DECIMAL':
			case 'NUMERIC':

				$this->parseFieldLengthDecimals(inout $tokens, inout $f);
				$this->parseFieldLength(inout $tokens, inout $f);
				$this->parseFieldUnsigned(inout $tokens, inout $f);
				$this->parseFieldZerofill(inout $tokens, inout $f);
				break;


			# BIT[(length)]
			# BINARY[(length)]
			case 'BIT':
			case 'BINARY':

				$this->parseFieldLength(inout $tokens, inout $f);
				break;


			# VARBINARY(length)
			case 'VARBINARY':

				$this->parseFieldLength(inout $tokens, inout $f);
				break;

			# CHAR[(length)] [CHARACTER SET charset_name] [COLLATE collation_name]
			case 'CHAR':

				$this->parseFieldLength(inout $tokens, inout $f);
				$this->parseFieldCharset(inout $tokens, inout $f);
				$this->parseFieldCollate(inout $tokens, inout $f);
				break;

			# VARCHAR(length) [CHARACTER SET charset_name] [COLLATE collation_name]
			case 'VARCHAR':

				$this->parseFieldLength(inout $tokens, inout $f);
				$this->parseFieldCharset(inout $tokens, inout $f);
				$this->parseFieldCollate(inout $tokens, inout $f);
				break;

			# TINYTEXT   [BINARY] [CHARACTER SET charset_name] [COLLATE collation_name]
			# TEXT       [BINARY] [CHARACTER SET charset_name] [COLLATE collation_name]
			# MEDIUMTEXT [BINARY] [CHARACTER SET charset_name] [COLLATE collation_name]
			# LONGTEXT   [BINARY] [CHARACTER SET charset_name] [COLLATE collation_name]
			case 'TINYTEXT':
			case 'TEXT':
			case 'MEDIUMTEXT':
			case 'LONGTEXT':

				# binary
				$this->parseFieldCharset(inout $tokens, inout $f);
				$this->parseFieldCollate(inout $tokens, inout $f);
				break;

			# ENUM(value1,value2,value3,...) [CHARACTER SET charset_name] [COLLATE collation_name]
			# SET (value1,value2,value3,...) [CHARACTER SET charset_name] [COLLATE collation_name]
			case 'ENUM':
			case 'SET':

				$f['values'] = $this->parseValueList(inout $tokens);
				$this->parseFieldCharset(inout $tokens, inout $f);
				$this->parseFieldCollate(inout $tokens, inout $f);
				break;

			default:
				die("Unsupported field type: {$f['type']}");
		}

		# [NOT NULL | NULL]
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'NOT NULL') {
			$f['null'] = false;
			$tokens = Vec\drop($tokens, 1);
		}
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'NULL') {
			$f['null'] = true;
			$tokens = Vec\drop($tokens, 1);
		}

		# [DEFAULT default_value]
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'DEFAULT') {
			$f['default'] = $this->decodeValue($tokens[1]);
			if ($f['default'] === 'NULL') {
				$f['null'] = true;
			}

			$tokens = Vec\drop($tokens, 1);
			$tokens = Vec\drop($tokens, 1);
		}

		# [AUTO_INCREMENT]
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'AUTO_INCREMENT') {
			$f['auto_increment'] = true;
			$tokens = Vec\drop($tokens, 1);
		}

		# [UNIQUE [KEY] | [PRIMARY] KEY]
		# [COMMENT 'string']
		# [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
		# [STORAGE {DISK|MEMORY|DEFAULT}]
		# [reference_definition]

		if (C\count($tokens)) {
			$f['more'] = $tokens;
		}

		$f as parsed_field;

		return $f;
	}

	private function parseTableProps(inout vec<string> $tokens): dict<string, string> {

		$alt_names = dict[
			'CHARACTER SET' => 'CHARSET',
			'DEFAULT CHARACTER SET' => 'CHARSET',
			'DEFAULT CHARSET' => 'CHARSET',
			'DEFAULT COLLATE' => 'COLLATE',
		];

		$props = dict[];
		$stop = false;

		while (C\count($tokens)) {
			if ($stop) {
				break;
			}

			switch (Str\uppercase($tokens[0])) {
				case 'ENGINE':
				case 'AUTO_INCREMENT':
				case 'AVG_ROW_LENGTH':
				#case 'CHECKSUM':
				case 'COMMENT':
				case 'CONNECTION':
				case 'DELAY_KEY_WRITE':
				case 'INSERT_METHOD':
				case 'KEY_BLOCK_SIZE':
				case 'MAX_ROWS':
				case 'MIN_ROWS':
				case 'PACK_KEYS':
				case 'PASSWORD':
				case 'ROW_FORMAT':
				case 'COLLATE':
				case 'CHARSET':
				case 'DATA DIRECTORY':
				case 'INDEX DIRECTORY':
					$t = $this->vecUnshift(inout $tokens);
					$prop = Str\uppercase($t);

					if ($tokens[0] === '=') {
						$tokens = Vec\drop($tokens, 1);
					}

					$t = $this->vecUnshift(inout $tokens);
					$props[$prop] = $t;

					if (!C\is_empty($tokens) && $tokens[0] === ',') {
						$tokens = Vec\drop($tokens, 1);
					}
					break;

				case 'CHARACTER SET':
				case 'DEFAULT COLLATE':
				case 'DEFAULT CHARACTER SET':
				case 'DEFAULT CHARSET':
					$t = $this->vecUnshift(inout $tokens);
					$prop = $alt_names[Str\uppercase($t)];
					if ($tokens[0] === '=') {
						$tokens = Vec\drop($tokens, 1);
					}

					$t = $this->vecUnshift(inout $tokens);
					$props[$prop] = $t;
					if (!C\is_empty($tokens) && $tokens[0] === ',') {
						$tokens = Vec\drop($tokens, 1);
					}
					break;

				default:
					$stop = true;
					break;
			}
		}

		return $props;
	}


	# Given the source map, extract the tokens from the original sql,
	# Along the way, simplify parsing by merging certain tokens when
	# they occur next to each other. MySQL treats these productions
	# equally: 'UNIQUE|UNIQUE INDEX|UNIQUE KEY' and if they are
	# all always a single token it makes parsing easier.

	private function extractTokens(string $sql, vec<(int, int)> $source_map): vec<string> {
		$lists = keyset[
			'FULLTEXT INDEX',
			'FULLTEXT KEY',
			'SPATIAL INDEX',
			'SPATIAL KEY',
			'FOREIGN KEY',
			'USING BTREE',
			'USING HASH',
			'PRIMARY KEY',
			'UNIQUE INDEX',
			'UNIQUE KEY',
			'CREATE TABLE',
			'CREATE TEMPORARY TABLE',
			'DATA DIRECTORY',
			'INDEX DIRECTORY',
			'DEFAULT CHARACTER SET',
			'CHARACTER SET',
			'DEFAULT CHARSET',
			'DEFAULT COLLATE',
			'IF NOT EXISTS',
			'NOT NULL',
			'WITH PARSER',
		];

		$singles = keyset[
			'NULL',
			'CONSTRAINT',
			'INDEX',
			'KEY',
			'UNIQUE',
		];


		$maps = dict[];
		foreach ($lists as $l) {
			$a = Str\split($l, ' ');
			if (!C\contains_key($maps, $a[0])) {
				$maps[$a[0]] = vec[];
			}
			$maps[$a[0]][] = $a;
		}
		$smap = dict[];
		foreach ($singles as $s) {
			$smap[$s] = 1;
		}

		$out = vec[];
		$out_map = vec[];

		$i = 0;
		$len = C\count($source_map);
		while ($i < $len) {
			$token = Str\slice($sql, $source_map[$i][0], $source_map[$i][1]);
			$tokenUpper = Str\uppercase($token);
			if (C\contains_key($maps, $tokenUpper)) {
				$found = false;
				foreach ($maps[$tokenUpper] as $list) {
					$fail = false;
					foreach ($list as $k => $v) {
						$next = Str\uppercase(Str\slice($sql, $source_map[$k + $i][0], $source_map[$k + $i][1]));
						if ($v !== $next) {
							$fail = true;
							break;
						}
					}
					if (!$fail) {
						$out[] = Str\join($list, ' ');

						# Extend the length of the first token to include everything
						# up through the last in the sequence.
						$j = $i + C\count($list) - 1;
						$out_map[] =
							tuple($source_map[$i][0], ($source_map[$j][0] - $source_map[$i][0]) + $source_map[$j][1]);

						$i = $j + 1;
						$found = true;
						break;
					}
				}
				if ($found) {
					continue;
				}
			}
			if (C\contains_key($smap, $tokenUpper)) {
				$out[] = $tokenUpper;
				$out_map[] = $source_map[$i];
				$i++;
				continue;
			}
			$out[] = $token;
			$out_map[] = $source_map[$i];
			$i++;
		}

		$this->sourceMap = $out_map;
		return $out;
	}

	private function parseIndexType(inout vec<string> $tokens, inout parsed_index $index): void {
		if (!C\is_empty($tokens) && $tokens[0] === 'USING BTREE') {
			$index['mode'] = 'btree';
			$tokens = Vec\drop($tokens, 1);
		}
		if (!C\is_empty($tokens) && $tokens[0] === 'USING HASH') {
			$index['mode'] = 'hash';
			$tokens = Vec\drop($tokens, 1);
		}
	}

	private function parseIndexColumns(inout vec<string> $tokens, inout parsed_index $index): void {

		# col_name [(length)] [ASC | DESC]

		if ($tokens[0] !== '(') {
			return;
		}
		$tokens = Vec\drop($tokens, 1);

		while (true) {

			$t = $this->vecUnshift(inout $tokens);
			$col = shape(
				'name' => $this->decodeIdentifier($t),
			);

			if ($tokens[0] === '(' && $tokens[2] === ')') {
				$col['length'] = (int)$tokens[1];
				$tokens = Vec\drop($tokens, 3);
			}

			if (Str\uppercase($tokens[0]) === 'ASC') {
				$col['direction'] = 'asc';
				$tokens = Vec\drop($tokens, 1);
			} elseif (Str\uppercase($tokens[0]) === 'DESC') {
				$col['direction'] = 'desc';
				$tokens = Vec\drop($tokens, 1);
			}

			$index['cols'][] = $col;

			if ($tokens[0] === ')') {
				$tokens = Vec\drop($tokens, 1);
				return;
			}

			if ($tokens[0] === ',') {
				$tokens = Vec\drop($tokens, 1);
				continue;
			}

			# hmm, an unexpected token
			return;
		}
	}

	private function parseIndexOptions(inout vec<string> $tokens, inout parsed_index $index): void {
		# index_option:
		#    KEY_BLOCK_SIZE [=] value
		#  | index_type
		#  | WITH PARSER parser_name

		if (!C\is_empty($tokens) && $tokens[0] === 'KEY_BLOCK_SIZE') {
			$tokens = Vec\drop($tokens, 1);
			if ($tokens[0] === '=') {
				Vec\drop($tokens, 1);
			}
			$index['key_block_size'] = $tokens[0];
			$tokens = Vec\drop($tokens, 1);
		}

		$this->parseIndexType(inout $tokens, inout $index);

		if (!C\is_empty($tokens) && $tokens[0] === 'WITH PARSER') {
			$index['parser'] = $tokens[1];
			$tokens = Vec\drop($tokens, 2);
		}
	}


	#
	# helper functions for parsing bits of field definitions
	#

	private function parseFieldLength(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && $tokens[0] === '(' && $tokens[2] === ')') {
			$f['length'] = $tokens[1];
			$tokens = Vec\drop($tokens, 3);
		}
	}

	private function parseFieldLengthDecimals(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && $tokens[0] === '(' && $tokens[2] === ',' && $tokens[4] === ')') {
			$f['length'] = $tokens[1];
			$f['decimals'] = $tokens[3];
			$tokens = Vec\drop($tokens, 5);
		}
	}

	private function parseFieldUnsigned(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'UNSIGNED') {
			$f['unsigned'] = true;
			$tokens = Vec\drop($tokens, 1);
		}
	}

	private function parseFieldZerofill(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'ZEROFILL') {
			$f['zerofill'] = true;
			$tokens = Vec\drop($tokens, 1);
		}
	}

	private function parseFieldCharset(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'CHARACTER SET') {
			$f['character_set'] = $tokens[1];
			$tokens = Vec\drop($tokens, 2);
		}
	}

	private function parseFieldCollate(inout vec<string> $tokens, inout parsed_field $f): void {
		if (!C\is_empty($tokens) && Str\uppercase($tokens[0]) === 'COLLATE') {
			$f['collation'] = $tokens[1];
			$tokens = Vec\drop($tokens, 2);
		}
	}

	private function parseValueList(inout vec<string> $tokens): ?vec<string> {
		if (C\is_empty($tokens) || $tokens[0] !== '(') {
			return null;
		}
		$tokens = Vec\drop($tokens, 1);

		$values = vec[];
		while (C\count($tokens)) {

			if ($tokens[0] === ')') {
				$tokens = Vec\drop($tokens, 1);
				return $values;
			}

			$t = $this->vecUnshift(inout $tokens);
			$values[] = $this->decodeValue($t);

			if ($tokens[0] === ')') {
				$tokens = Vec\drop($tokens, 1);
				return $values;
			}

			if ($tokens[0] === ',') {
				$tokens = Vec\drop($tokens, 1);
			} else {
				# error
				return $values;
			}
		}
		return null;
	}

	private function decodeIdentifier(string $token): string {
		if ($token[0] === '`') {
			return Str\strip_prefix($token, '`') |> Str\strip_suffix($$, '`');
		}
		return $token;
	}

	private function decodeValue(string $token): string {

		#
		# decode strings
		#

		if ($token[0] === "'" || $token[0] === '"') {
			$map = dict[
				'n' => "\n",
				'r' => "\r",
				't' => "\t",
			];
			$out = '';
			for ($i = 1; $i < Str\length($token) - 1; $i++) {
				if ($token[$i] === '\\') {
					if (C\contains_key($map, $token[$i + 1])) {
						$out .= $map[$token[$i + 1]];
					} else {
						$out .= $token[$i + 1];
					}
					$i++;
				} else {
					$out .= $token[$i];
				}
			}
			return $out;
		}

		return $token;
	}

	private function vecUnshift(inout vec<string> $tokens): string {
		$t = C\firstx($tokens);
		$tokens = Vec\drop($tokens, 1);
		return $t;
	}
}
