<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;


final class CreateTableParser
{
    /**
     * @return array<string, array{name:string, fields:array<int, array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}>, sql:string, indexes:array<int, array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string}>, props:array<string, string>}>
     */
    public function parse(string $sql)
    {
        $this->lex($sql);
        return $this->walk($this->tokens, $sql, $this->sourceMap);
    }

    /**
     * @var array<int, string>
     */
    private $tokens = [];

    /**
     * @var array<int, array{0:int, 1:int}>
     */
    private $sourceMap = [];

    /**
     * @return array<int, string>
     */
    private function lex(string $sql)
    {
        $this->sourceMap = $this->lexImpl($sql);
        $this->tokens = $this->extractTokens($sql, $this->sourceMap);
        return $this->tokens;
    }

    /**
     * @return array<int, array{0:int, 1:int}>
     */
    private function lexImpl(string $sql)
    {
        $pos = 0;
        $len = \strlen($sql);
        $source_map = [];
        while ($pos < $len) {
            $match = Regex\first_match($sql, "!s+!A", $pos);
            if ($match !== null) {
                $pos += \strlen($match[0]);
                continue;
            }
            if (Regex\matches($sql, "!--!A", $pos)) {
                $p2 = \strpos($sql, "\n", $pos);
                if ($p2 === false) {
                    $pos = $len;
                } else {
                    $pos = $p2 + 1;
                }
                continue;
            }
            if (Regex\matches($sql, "!\\*!A", $pos)) {
                $p2 = \strpos($sql, "*/", $pos);
                if ($p2 === false) {
                    $pos = $len;
                } else {
                    $pos = $p2 + 2;
                }
                continue;
            }
            $match = Regex\first_match($sql, "![[:alpha:]][[:alnum:]_]*!A", $pos);
            if ($match !== null) {
                $source_map[] = [$pos, \strlen($match[0] ?? '')];
                $pos += \strlen($match[0]);
                continue;
            }
            if (\substr($sql, $pos, 1) === '`') {
                $p2 = \strpos($sql, "`", $pos + 1);
                if ($p2 === false) {
                    $pos = $len;
                } else {
                    $source_map[] = [$pos, 1 + $p2 - $pos];
                    $pos = $p2 + 1;
                }
                continue;
            }
            $match = Regex\first_match($sql, "!(d+.?d*|.d+)!A", $pos);
            if ($match !== null) {
                $source_map[] = [$pos, \strlen($match[0])];
                $pos += \strlen($match[0]);
                continue;
            }
            if ($sql[$pos] === "'" || $sql[$pos] === '"') {
                $c = $pos + 1;
                $q = $sql[$pos];
                while ($c < \strlen($sql)) {
                    if ($sql[$c] === '\\') {
                        $c += 2;
                        continue;
                    }
                    if ($sql[$c] === $q) {
                        $slen = $c + 1 - $pos;
                        $source_map[] = [$pos, $slen];
                        $pos += $slen;
                        break;
                    }
                    $c++;
                }
                continue;
            }
            $source_map[] = [$pos, 1];
            $pos++;
        }
        return $source_map;
    }

    /**
     * @param list<string> $tokens
     * @param array<int, array{0:int, 1:int}> $source_map
     *
     * @return array<string, array{name:string, fields:array<int, array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}>, sql:string, indexes:array<int, array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string}>, props:array<string, string>}>
     */
    private function walk(array $tokens, string $sql, array $source_map)
    {
        $statements = [];
        $temp = [];
        $start = 0;
        for ($i = 0; $i < \count($tokens); $i++) {
            $t = $tokens[$i];
            if ($t === ';') {
                if (\count($temp)) {
                    $statements[] = [
                        "tuples" => $temp,
                        "sql" => \substr($sql, $source_map[$start][0], $source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1])
                    ];
                }
                $temp = [];
                $start = $i + 1;
            } else {
                $temp[] = $t;
            }
        }

        if (\count($temp)) {
            $statements[] = [
                "tuples" => $temp,
                "sql" => \substr($sql, $source_map[$start][0], $source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1])
            ];
        }

        $tables = [];
        foreach ($statements as $stmt) {
            $s = $stmt['tuples'];
            if (\strtoupper($s[0]) === 'CREATE TABLE') {
                \array_shift($s);
                $table = $this->parseCreateTable($s, $stmt['sql']);
                $tables[$table['name']] = $table;
            }
            if (\strtoupper($s[0]) === 'CREATE TEMPORARY TABLE') {
                \array_shift($s);
                $table = $this->parseCreateTable($s, $stmt['sql']);
                $table['props']['temp'] = '1';
                $tables[$table['name']] = $table;
            }
        }

        return $tables;
    }

    /**
     * @param list<string> $tokens
     *
     * @return array{name:string, fields:array<int, array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}>, sql:string, indexes:array<int, array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string}>, props:array<string, string>}
     */
    private function parseCreateTable(array $tokens, string $sql)
    {
        if ($tokens[0] === 'IF NOT EXISTS') {
            \array_shift($tokens);
        }
        $t = \array_shift($tokens);
        $name = $this->decodeIdentifier($t);
        if ($this->nextTokenIs($tokens, 'LIKE')) {
            \array_shift($tokens);
            $t = \array_shift($tokens);
            $old_name = $this->decodeIdentifier($t);
            return ['name' => $name, 'sql' => $sql, 'props' => ['like' => $old_name], 'fields' => [], 'indexes' => []];
        }
        $fields = [];
        $indexes = [];
        if ($this->nextTokenIs($tokens, '(')) {
            \array_shift($tokens);
            $ret = $this->parseCreateDefinition($tokens);
            $fields = $ret['fields'];
            $indexes = $ret['indexes'];
        }
        $props = $this->parseTableProps($tokens);
        $table = ['name' => $name, 'fields' => $fields, 'indexes' => $indexes, 'props' => $props, 'sql' => $sql];
        return $table;
    }

    /**
     * @param list<string> $tokens
     *
     * @return bool
     */
    private function nextTokenIs(array $tokens, string $val)
    {
        return \strtoupper($tokens[0]) === $val;
    }

    /**
     * @param list<string> $tokens
     *
     * @return array{fields:array<int, array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}>, indexes:array<int, array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string}>}
     */
    private function parseCreateDefinition(array &$tokens)
    {
        $fields = [];
        $indexes = [];

        while ($tokens[0] !== ')') {
            $these_tokens = $this->sliceUntilNextField($tokens);
            $this->parseFieldOrKey($these_tokens, $fields, $indexes);
        }

        \array_shift($tokens);
        return ['fields' => $fields, 'indexes' => $indexes];
    }

    /**
     * @param list<string> $tokens
     * @param array<int, array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}> $fields
     * @param array<int, array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string}> $indexes
     *
     * @return void
     */
    private function parseFieldOrKey(array &$tokens, array &$fields, array &$indexes)
    {
        $has_constraint = false;
        $constraint = null;

        if ($tokens[0] === 'CONSTRAINT') {
            $has_constraint = true;

            if ($tokens[1] === 'PRIMARY KEY'
                || $tokens[1] === 'UNIQUE'
                || $tokens[1] === 'UNIQUE KEY'
                || $tokens[1] === 'UNIQUE INDEX'
                || $tokens[1] === 'FOREIGN KEY'
            ) {
                \array_shift($tokens);
            } else {
                \array_shift($tokens);
                $constraint = \array_shift($tokens);
            }
        }

        switch ($tokens[0]) {
            case 'INDEX':
            case 'KEY':
            case 'UNIQUE':
            case 'UNIQUE INDEX':
            case 'UNIQUE KEY':
                $index = ['type' => 'INDEX', 'cols' => []];
                if ($tokens[0] === 'UNIQUE' || $tokens[0] === 'UNIQUE INDEX' || $tokens[0] === 'UNIQUE KEY') {
                    $index['type'] = 'UNIQUE';
                }
                \array_shift($tokens);
                if ($tokens[0] !== '(' && $tokens[0] !== 'USING BTREE' && $tokens[0] !== 'USING HASH') {
                    $t = \array_shift($tokens);
                    $index['name'] = $this->decodeIdentifier($t);
                }
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index['more'] = $tokens;
                }
                $indexes[] = $index;
                return;

            case 'PRIMARY KEY':
                $index = ['type' => 'PRIMARY', 'cols' => []];
                \array_shift($tokens);
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index['more'] = $tokens;
                }
                $indexes[] = $index;
                return;

            case 'FULLTEXT':
            case 'FULLTEXT INDEX':
            case 'FULLTEXT KEY':
            case 'SPATIAL':
            case 'SPATIAL INDEX':
            case 'SPATIAL KEY':
                $index = ['type' => 'FULLTEXT', 'cols' => []];

                if ($tokens[0] === 'SPATIAL' || $tokens[0] === 'SPATIAL INDEX' || $tokens[0] === 'SPATIAL KEY') {
                    $index['type'] = 'SPATIAL';
                }

                \array_shift($tokens);
                if ($tokens[0] !== '(') {
                    $t = \array_shift($tokens);
                    $index['name'] = $this->decodeIdentifier($t);
                }
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index['more'] = $tokens;
                }
                $indexes[] = $index;
                return;
            case 'CHECK':
                return;
        }

        $fields[] = $this->parseField($tokens);
    }

    /**
     * @param list<string> $tokens
     *
     * @return array<int, string>
     */
    private function sliceUntilNextField(array &$tokens)
    {
        $out = [];
        $stack = 0;
        while (\count($tokens)) {
            $next = $tokens[0];
            if ($next === '(') {
                $stack++;
                $t = \array_shift($tokens);
                $out[] = $t;
            } else {
                if ($next === ')') {
                    if ($stack) {
                        $stack--;
                        $t = \array_shift($tokens);
                        $out[] = $t;
                    } else {
                        return $out;
                    }
                } else {
                    if ($next === ',') {
                        if ($stack) {
                            $t = \array_shift($tokens);
                            $out[] = $t;
                        } else {
                            \array_shift($tokens);
                            return $out;
                        }
                    } else {
                        $t = \array_shift($tokens);
                        $out[] = $t;
                    }
                }
            }
        }
        return $out;
    }

    /**
     * @param list<string> $tokens
     *
     * @return array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string}
     */
    private function parseField(array &$tokens)
    {
        $t = \array_shift($tokens);
        $t2 = \array_shift($tokens);
        $f = ['name' => $this->decodeIdentifier($t), 'type' => \strtoupper($t2)];
        switch ($f['type']) {
            case 'DATE':
            case 'TIME':
            case 'TIMESTAMP':
            case 'DATETIME':
            case 'YEAR':
            case 'TINYBLOB':
            case 'BLOB':
            case 'MEDIUMBLOB':
            case 'LONGBLOB':
                break;
            case 'TINYINT':
            case 'SMALLINT':
            case 'MEDIUMINT':
            case 'INT':
            case 'INTEGER':
            case 'BIGINT':
                $this->parseFieldLength($tokens, $f);
                $this->parseFieldUnsigned($tokens, $f);
                $this->parseFieldZerofill($tokens, $f);
                break;
            case 'REAL':
            case 'DOUBLE':
            case 'FLOAT':
                $this->parseFieldLengthDecimals($tokens, $f);
                $this->parseFieldUnsigned($tokens, $f);
                $this->parseFieldZerofill($tokens, $f);
                break;
            case 'DECIMAL':
            case 'NUMERIC':
                $this->parseFieldLengthDecimals($tokens, $f);
                $this->parseFieldLength($tokens, $f);
                $this->parseFieldUnsigned($tokens, $f);
                $this->parseFieldZerofill($tokens, $f);
                break;
            case 'BIT':
            case 'BINARY':
                $this->parseFieldLength($tokens, $f);
                break;
            case 'VARBINARY':
                $this->parseFieldLength($tokens, $f);
                break;
            case 'CHAR':
                $this->parseFieldLength($tokens, $f);
                $this->parseFieldCharset($tokens, $f);
                $this->parseFieldCollate($tokens, $f);
                break;
            case 'VARCHAR':
                $this->parseFieldLength($tokens, $f);
                $this->parseFieldCharset($tokens, $f);
                $this->parseFieldCollate($tokens, $f);
                break;
            case 'TINYTEXT':
            case 'TEXT':
            case 'MEDIUMTEXT':
            case 'LONGTEXT':
                $this->parseFieldCharset($tokens, $f);
                $this->parseFieldCollate($tokens, $f);
                break;
            case 'ENUM':
            case 'SET':
                $f['values'] = $this->parseValueList($tokens);
                $this->parseFieldCharset($tokens, $f);
                $this->parseFieldCollate($tokens, $f);
                break;
            default:
                die("Unsupported field type: {$f['type']}");
        }
        if ($tokens && \strtoupper($tokens[0]) === 'NOT NULL') {
            $f['null'] = false;
            \array_shift($tokens);
        }
        if (($tokens) && \strtoupper($tokens[0]) === 'NULL') {
            $f['null'] = true;
            \array_shift($tokens);
        }
        if (($tokens) && \strtoupper($tokens[0]) === 'DEFAULT') {
            $f['default'] = $this->decodeValue($tokens[1]);
            if ($f['default'] === 'NULL') {
                $f['null'] = true;
            }
            \array_shift($tokens);
            \array_shift($tokens);
        }
        if ($tokens && \strtoupper($tokens[0]) === 'AUTO_INCREMENT') {
            $f['auto_increment'] = true;
            \array_shift($tokens);
        }
        if (\count($tokens)) {
            $f['more'] = $tokens;
        }
        return $f;
    }

    /**
     * @param list<string> $tokens
     *
     * @return array<string, string>
     */
    private function parseTableProps(array &$tokens)
    {
        $alt_names = ['CHARACTER SET' => 'CHARSET', 'DEFAULT CHARACTER SET' => 'CHARSET', 'DEFAULT CHARSET' => 'CHARSET', 'DEFAULT COLLATE' => 'COLLATE'];
        $props = [];
        $stop = false;
        while (\count($tokens)) {
            if ($stop) {
                break;
            }
            switch (\strtoupper($tokens[0])) {
                case 'ENGINE':
                case 'AUTO_INCREMENT':
                case 'AVG_ROW_LENGTH':
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
                    $t = \array_shift($tokens);
                    $prop = \strtoupper($t);
                    if ($tokens[0] === '=') {
                        \array_shift($tokens);
                    }
                    $t = \array_shift($tokens);
                    $props[$prop] = $t;
                    if (($tokens) && $tokens[0] === ',') {
                        \array_shift($tokens);
                    }
                    break;
                case 'CHARACTER SET':
                case 'DEFAULT COLLATE':
                case 'DEFAULT CHARACTER SET':
                case 'DEFAULT CHARSET':
                    $t = \array_shift($tokens);
                    $prop = $alt_names[\strtoupper($t)];
                    if ($tokens[0] === '=') {
                        \array_shift($tokens);
                    }
                    $t = \array_shift($tokens);
                    $props[$prop] = $t;
                    if (($tokens) && $tokens[0] === ',') {
                        \array_shift($tokens);
                    }
                    break;
                default:
                    $stop = true;
                    break;
            }
        }
        return $props;
    }

    /**
     * @param array<int, array{0:int, 1:int}> $source_map
     *
     * @return array<int, string>
     */
    private function extractTokens(string $sql, array $source_map)
    {
        $lists = ['FULLTEXT INDEX' => 'FULLTEXT INDEX', 'FULLTEXT KEY' => 'FULLTEXT KEY', 'SPATIAL INDEX' => 'SPATIAL INDEX', 'SPATIAL KEY' => 'SPATIAL KEY', 'FOREIGN KEY' => 'FOREIGN KEY', 'USING BTREE' => 'USING BTREE', 'USING HASH' => 'USING HASH', 'PRIMARY KEY' => 'PRIMARY KEY', 'UNIQUE INDEX' => 'UNIQUE INDEX', 'UNIQUE KEY' => 'UNIQUE KEY', 'CREATE TABLE' => 'CREATE TABLE', 'CREATE TEMPORARY TABLE' => 'CREATE TEMPORARY TABLE', 'DATA DIRECTORY' => 'DATA DIRECTORY', 'INDEX DIRECTORY' => 'INDEX DIRECTORY', 'DEFAULT CHARACTER SET' => 'DEFAULT CHARACTER SET', 'CHARACTER SET' => 'CHARACTER SET', 'DEFAULT CHARSET' => 'DEFAULT CHARSET', 'DEFAULT COLLATE' => 'DEFAULT COLLATE', 'IF NOT EXISTS' => 'IF NOT EXISTS', 'NOT NULL' => 'NOT NULL', 'WITH PARSER' => 'WITH PARSER'];
        $singles = ['NULL' => 'NULL', 'CONSTRAINT' => 'CONSTRAINT', 'INDEX' => 'INDEX', 'KEY' => 'KEY', 'UNIQUE' => 'UNIQUE'];
        $maps = [];
        foreach ($lists as $l) {
            $a = \explode(' ', $l);
            if (!\array_key_exists($a[0], $maps)) {
                $maps[$a[0]] = [];
            }
            $maps[$a[0]][] = $a;
        }
        $smap = [];
        foreach ($singles as $s) {
            $smap[$s] = 1;
        }
        $out = [];
        $out_map = [];
        $i = 0;
        $len = \count($source_map);
        while ($i < $len) {
            $token = \substr($sql, $source_map[$i][0], $source_map[$i][1]);
            $tokenUpper = \strtoupper($token);
            if (\array_key_exists($tokenUpper, $maps)) {
                $found = false;
                foreach ($maps[$tokenUpper] as $list) {
                    $fail = false;
                    foreach ($list as $k => $v) {
                        $next = \strtoupper(\substr($sql, $source_map[$k + $i][0], $source_map[$k + $i][1]));
                        if ($v !== $next) {
                            $fail = true;
                            break;
                        }
                    }
                    if (!$fail) {
                        $out[] = \implode(' ', $list);
                        $j = $i + \count($list) - 1;
                        $out_map[] = [$source_map[$i][0], $source_map[$j][0] - $source_map[$i][0] + $source_map[$j][1]];
                        $i = $j + 1;
                        $found = true;
                        break;
                    }
                }
                if ($found) {
                    continue;
                }
            }
            if (\array_key_exists($tokenUpper, $smap)) {
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

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string} $index
     *
     * @return void
     */
    private function parseIndexType(array &$tokens, array &$index)
    {
        if (($tokens) && $tokens[0] === 'USING BTREE') {
            $index['mode'] = 'btree';
            \array_shift($tokens);
        }
        if (($tokens) && $tokens[0] === 'USING HASH') {
            $index['mode'] = 'hash';
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string} $index
     *
     * @return void
     */
    private function parseIndexColumns(array &$tokens, array &$index)
    {
        if ($tokens[0] !== '(') {
            return;
        }
        \array_shift($tokens);
        while (true) {
            $t = \array_shift($tokens);
            $col = ['name' => $this->decodeIdentifier($t)];
            if ($tokens[0] === '(' && $tokens[2] === ')') {
                $col['length'] = (int) $tokens[1];
                $tokens = \array_slice($tokens, 3);
            }
            if (\strtoupper($tokens[0]) === 'ASC') {
                $col['direction'] = 'asc';
                \array_shift($tokens);
            } else {
                if (\strtoupper($tokens[0]) === 'DESC') {
                    $col['direction'] = 'desc';
                    \array_shift($tokens);
                }
            }
            $index['cols'][] = $col;
            if ($tokens[0] === ')') {
                \array_shift($tokens);
                return;
            }
            if ($tokens[0] === ',') {
                \array_shift($tokens);
                continue;
            }
            return;
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, cols:array<int, array{name:string, length:int, direction:string}>, mode:string, parser:string, more:mixed, key_block_size:string} $index
     *
     * @return void
     */
    private function parseIndexOptions(array $tokens, array $index)
    {
        if (($tokens) && $tokens[0] === 'KEY_BLOCK_SIZE') {
            \array_shift($tokens);
            if ($tokens[0] === '=') {
                \array_shift($tokens);
            }
            $index['key_block_size'] = $tokens[0];
            \array_shift($tokens);
        }
        $this->parseIndexType($tokens, $index);
        if (($tokens) && $tokens[0] === 'WITH PARSER') {
            $index['parser'] = $tokens[1];
            \array_shift($tokens); \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldLength(array $tokens, array $f)
    {
        if (($tokens) && $tokens[0] === '(' && $tokens[2] === ')') {
            $f['length'] = $tokens[1];
            $tokens = \array_slice($tokens, 3);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldLengthDecimals(array $tokens, array $f)
    {
        if (($tokens) && $tokens[0] === '(' && $tokens[2] === ',' && $tokens[4] === ')') {
            $f['length'] = $tokens[1];
            $f['decimals'] = $tokens[3];
            $tokens = \array_slice($tokens, 5);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldUnsigned(array $tokens, array $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'UNSIGNED') {
            $f['unsigned'] = true;
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldZerofill(array &$tokens, array $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'ZEROFILL') {
            $f['zerofill'] = true;
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldCharset(array &$tokens, array $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'CHARACTER SET') {
            $f['character_set'] = $tokens[1];
            \array_shift($tokens);
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     * @param array{name:string, type:string, length:string, unsigned:bool, null:bool, default:string} $f
     *
     * @return void
     */
    private function parseFieldCollate(array &$tokens, array $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'COLLATE') {
            $f['collation'] = $tokens[1];
            \array_shift($tokens);
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return array<int, string>|null
     */
    private function parseValueList(array $tokens)
    {
        if (!($tokens) || $tokens[0] !== '(') {
            return null;
        }
        \array_shift($tokens);
        $values = [];
        while (\count($tokens)) {
            if ($tokens[0] === ')') {
                \array_shift($tokens);
                return $values;
            }
            $t = \array_shift($tokens);
            $values[] = $this->decodeValue($t);
            if ($tokens[0] === ')') {
                \array_shift($tokens);
                return $values;
            }
            if ($tokens[0] === ',') {
                \array_shift($tokens);
            } else {
                return $values;
            }
        }
        return null;
    }

    /**
     * @return string
     */
    private function decodeIdentifier(string $token)
    {
        if ($token[0] === '`') {
            return Str\strip_suffix(Str\strip_prefix($token, '`'), '`');
        }
        return $token;
    }

    /**
     * @return string
     */
    private function decodeValue(string $token)
    {
        if ($token[0] === "'" || $token[0] === '"') {
            $map = ['n' => "\n", 'r' => "\r", 't' => "\t"];
            $out = '';
            for ($i = 1; $i < \strlen($token) - 1; $i++) {
                if ($token[$i] === '\\') {
                    if (\array_key_exists($token[$i + 1], $map)) {
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
}

