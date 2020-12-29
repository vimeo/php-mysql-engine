<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\CreateColumn;
use Vimeo\MysqlEngine\Query\CreateIndex;
use Vimeo\MysqlEngine\Query\CreateQuery;


final class CreateTableParser
{
    /**
     * @return array<string, CreateQuery>
     */
    public function parse(string $sql) : array
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
            \preg_match("!s+!A", $sql, $matches, 0, $pos);
            if ($matches) {
                $pos += \strlen($matches[0]);
                continue;
            }
            if (\preg_match("!--!A", $sql, $matches, 0, $pos)) {
                $p2 = \strpos($sql, "\n", $pos);
                if ($p2 === false) {
                    $pos = $len;
                } else {
                    $pos = $p2 + 1;
                }
                continue;
            }
            if (\preg_match("!\\*!A", $sql, $matches, 0, $pos)) {
                $p2 = \strpos($sql, "*/", $pos);
                if ($p2 === false) {
                    $pos = $len;
                } else {
                    $pos = $p2 + 2;
                }
                continue;
            }
            \preg_match("![[:alpha:]][[:alnum:]_]*!A", $sql, $matches, 0, $pos);
            if ($matches) {
                $source_map[] = [$pos, \strlen($matches[0])];
                $pos += \strlen($matches[0]);
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
            $match = \preg_match("!(d+.?d*|.d+)!A", $sql, $matches, 0, $pos);
            if ($matches) {
                $source_map[] = [$pos, \strlen($matches[0])];
                $pos += \strlen($matches[0]);
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
     * @param list<string>                $tokens
     * @param array<int, array{int, int}> $source_map
     *
     * @return array<string, CreateQuery>
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
                        "sql" => \substr(
                            $sql,
                            $source_map[$start][0],
                            $source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1]
                        )
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
                "sql" => \substr(
                    $sql,
                    $source_map[$start][0],
                    $source_map[$i][0] - $source_map[$start][0] + $source_map[$i][1]
                )
            ];
        }

        $tables = [];
        foreach ($statements as $stmt) {
            $s = $stmt['tuples'];
            if (\strtoupper($s[0]) === 'CREATE TABLE') {
                \array_shift($s);
                $table = $this->parseCreateTable($s, $stmt['sql']);
                $tables[$table->name] = $table;
            }
            if (\strtoupper($s[0]) === 'CREATE TEMPORARY TABLE') {
                \array_shift($s);
                $table = $this->parseCreateTable($s, $stmt['sql']);
                $table->props['temp'] = '1';
                $tables[$table->name] = $table;
            }
        }

        return $tables;
    }

    /**
     * @param list<string> $tokens
     */
    private function parseCreateTable(array $tokens, string $sql) : CreateQuery
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
            $q = new CreateQuery();
            $q->name = $name;
            $q->sql = $sql;
            $q->props = ['like' => $old_name];

            return $q;
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

        $q = new CreateQuery();
        $q->name = $name;
        $q->sql = $sql;
        $q->fields = $fields;
        $q->indexes = $indexes;
        $q->props = $props;

        return $q;
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
     * @return array{fields: array<int, CreateColumn>, indexes: array<int, CreateIndex>}
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
     * @param array<int, CreateColumn> $fields
     * @param array<int, CreateIndex> $indexes
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
                $index = new CreateIndex();
                $index->type = 'INDEX';
                if ($tokens[0] === 'UNIQUE' || $tokens[0] === 'UNIQUE INDEX' || $tokens[0] === 'UNIQUE KEY') {
                    $index->type = 'UNIQUE';
                }
                \array_shift($tokens);
                if ($tokens[0] !== '(' && $tokens[0] !== 'USING BTREE' && $tokens[0] !== 'USING HASH') {
                    $t = \array_shift($tokens);
                    $index->name = $this->decodeIdentifier($t);
                }
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index->more = $tokens;
                }
                $indexes[] = $index;
                return;

            case 'PRIMARY KEY':
                $index = new CreateIndex();
                $index->type = 'PRIMARY';
                \array_shift($tokens);
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index->more = $tokens;
                }
                $indexes[] = $index;
                return;

            case 'FULLTEXT':
            case 'FULLTEXT INDEX':
            case 'FULLTEXT KEY':
            case 'SPATIAL':
            case 'SPATIAL INDEX':
            case 'SPATIAL KEY':
                $index = new CreateIndex();
                $index->type = 'FULLTEXT';

                if ($tokens[0] === 'SPATIAL' || $tokens[0] === 'SPATIAL INDEX' || $tokens[0] === 'SPATIAL KEY') {
                    $index->type = 'SPATIAL';
                }

                \array_shift($tokens);
                if ($tokens[0] !== '(') {
                    $t = \array_shift($tokens);
                    $index->name = $this->decodeIdentifier($t);
                }
                $this->parseIndexType($tokens, $index);
                $this->parseIndexColumns($tokens, $index);
                $this->parseIndexOptions($tokens, $index);
                if (\count($tokens)) {
                    $index->more = $tokens;
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
     */
    private function parseField(array &$tokens) : CreateColumn
    {
        $t = \array_shift($tokens);
        $t2 = \array_shift($tokens);
        $f = new CreateColumn();
        $f->name = $this->decodeIdentifier($t);
        $f->type = \strtoupper($t2);
        switch ($f->type) {
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
                $f->values = $this->parseValueList($tokens);
                $this->parseFieldCharset($tokens, $f);
                $this->parseFieldCollate($tokens, $f);
                break;
            default:
                die("Unsupported field type: {$f->type}");
        }
        if ($tokens && \strtoupper($tokens[0]) === 'NOT NULL') {
            $f->null = false;
            \array_shift($tokens);
        }
        if (($tokens) && \strtoupper($tokens[0]) === 'NULL') {
            $f->null = true;
            \array_shift($tokens);
        }
        if (($tokens) && \strtoupper($tokens[0]) === 'DEFAULT') {
            $f->default = $this->decodeValue($tokens[1]);
            if ($f->default === 'NULL') {
                $f->null = true;
            }
            \array_shift($tokens);
            \array_shift($tokens);
        }
        if ($tokens && \strtoupper($tokens[0]) === 'AUTO_INCREMENT') {
            $f->auto_increment = true;
            \array_shift($tokens);
        }
        if (\count($tokens)) {
            $f->more = $tokens;
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
        $alt_names = [
            'CHARACTER SET' => 'CHARSET',
            'DEFAULT CHARACTER SET' => 'CHARSET',
            'DEFAULT CHARSET' => 'CHARSET',
            'DEFAULT COLLATE' => 'COLLATE'
        ];

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
        $lists = [
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
            'WITH PARSER'
        ];
        
        $maps = [];
        foreach ($lists as $l) {
            $a = \explode(' ', $l);
            if (!\array_key_exists($a[0], $maps)) {
                $maps[$a[0]] = [];
            }
            $maps[$a[0]][] = $a;
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
                        $out_map[] = [
                            $source_map[$i][0],
                            $source_map[$j][0] - $source_map[$i][0] + $source_map[$j][1]
                        ];
                        $i = $j + 1;
                        $found = true;
                        break;
                    }
                }
                if ($found) {
                    continue;
                }
            }
            if ($tokenUpper === 'NULL'
                || $tokenUpper === 'CONSTRAINT'
                || $tokenUpper === 'INDEX'
                || $tokenUpper === 'KEY'
                || $tokenUpper === 'UNIQUE'
            ) {
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
     * @param CreateIndex $index
     *
     * @return void
     */
    private function parseIndexType(array &$tokens, CreateIndex $index)
    {
        if (($tokens) && $tokens[0] === 'USING BTREE') {
            $index->mode = 'btree';
            \array_shift($tokens);
        }

        if (($tokens) && $tokens[0] === 'USING HASH') {
            $index->mode = 'hash';
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseIndexColumns(array &$tokens, CreateIndex $index)
    {
        if ($tokens[0] !== '(') {
            return;
        }

        \array_shift($tokens);

        while (true) {
            $t = \array_shift($tokens);
            $col = ['name' => $this->decodeIdentifier($t), 'cols' => []];
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
            $index->cols[] = $col;
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
     * @param CreateIndex $index
     *
     * @return void
     */
    private function parseIndexOptions(array $tokens, CreateIndex $index)
    {
        if (($tokens) && $tokens[0] === 'KEY_BLOCK_SIZE') {
            \array_shift($tokens);
            if ($tokens[0] === '=') {
                \array_shift($tokens);
            }
            $index->key_block_size = $tokens[0];
            \array_shift($tokens);
        }

        $this->parseIndexType($tokens, $index);

        if (($tokens) && $tokens[0] === 'WITH PARSER') {
            $index->parser = $tokens[1];
            \array_shift($tokens);
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldLength(array $tokens, CreateColumn $f)
    {
        if (($tokens) && $tokens[0] === '(' && $tokens[2] === ')') {
            $f->length = (int) $tokens[1];
            $tokens = \array_slice($tokens, 3);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldLengthDecimals(array $tokens, CreateColumn $f)
    {
        if (($tokens) && $tokens[0] === '(' && $tokens[2] === ',' && $tokens[4] === ')') {
            $f->length = (int) $tokens[1];
            $f->decimals = (int) $tokens[3];
            $tokens = \array_slice($tokens, 5);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldUnsigned(array $tokens, CreateColumn $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'UNSIGNED') {
            $f->unsigned = true;
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldZerofill(array &$tokens, CreateColumn $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'ZEROFILL') {
            $f->zerofill = true;
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldCharset(array &$tokens, CreateColumn $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'CHARACTER SET') {
            $f->character_set = $tokens[1];
            \array_shift($tokens);
            \array_shift($tokens);
        }
    }

    /**
     * @param list<string> $tokens
     *
     * @return void
     */
    private function parseFieldCollate(array &$tokens, CreateColumn $f)
    {
        if (($tokens) && \strtoupper($tokens[0]) === 'COLLATE') {
            $f->collation = $tokens[1];
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
            return \substr($token, 1, -1);
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
