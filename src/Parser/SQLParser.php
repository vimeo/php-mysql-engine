<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Query\{
    SelectQuery,
    DeleteQuery,
    TruncateQuery,
    InsertQuery,
    UpdateQuery,
    DropTableQuery,
    ShowTablesQuery
};

final class SQLParser
{
    /**
     * @var array<string, string>
     */
    const CLAUSES = [
        'SELECT' => true,
        'FROM' => true,
        'WHERE' => true,
        'GROUP' => true,
        'HAVING' => true,
        'LIMIT' => true,
        'ORDER' => true,
        'UPDATE' => true,
        'SET' => true,
        'DELETE' => true,
        'UNION' => true,
        'EXCEPT' => true,
        'INTERSECT' => true,
        'INSERT' => true,
        'VALUES' => true,
        'DROP' => true,
        'SHOW' => true,
    ];

    /**
     * @var array<string, string>
     */
    const SEPARATORS = [
        ')' => true,
        ',' => true,
        ';' => true
    ];

    /**
     * @var array<string, string>
     */
    const OPERATORS = [
        'INTERVAL' => true,
        'COLLATE' => true,
        '!' => true,
        '~' => true,
        '^' => true,
        '*' => true,
        '/' => true,
        'DIV' => true,
        '%' => true,
        'MOD' => true,
        '-' => true,
        '+' => true,
        '<<' => true,
        '>>' => true,
        '&' => true,
        '|' => true,
        '=' => true,
        '<=>' => true,
        '>=' => true,
        '>' => true,
        '<=' => true,
        '<' => true,
        '<>' => true,
        '!=' => true,
        'IS' => true,
        'LIKE' => true,
        'REGEXP' => true,
        'IN' => true,
        'EXISTS' => true,
        'BETWEEN' => true,
        'CASE' => true,
        'WHEN' => true,
        'THEN' => true,
        'ELSE' => true,
        'END' => true,
        'NOT' => true,
        'AND' => true,
        '&&' => true,
        'XOR' => true,
        'OR' => true,
        '||' => true,
        ':=' => true,
    ];

    /**
     * @var array<string, string>
     */
    const RESERVED_WORDS = [
        'ASC' => true,
        'DESC' => true,
        'AS' => true,
        'WITH' => true,
        'ON' => true,
        'OFFSET' => true,
        'BY' => true,
        'INTO' => true,
        'ALL' => true,
        'DISTINCT' => true,
        'DISTINCTROW' => true,
        'SQL_CALC_FOUND_ROWS' => true,
        'HIGH_PRIORITY' => true,
        'SQL_SMALL_RESULT' => true,
        'SQL_BIG_RESULT' => true,
        'SQL_BUFFER_RESULT' => true,
        'SQL_CACHE' => true,
        'SQL_NO_CACHE' => true,
        'JOIN' => true,
        'INNER' => true,
        'OUTER' => true,
        'LEFT' => true,
        'RIGHT' => true,
        'STRAIGHT_JOIN' => true,
        'NATURAL' => true,
        'USING' => true,
        'CROSS' => true,
        'USE' => true,
        'IGNORE' => true,
        'FORCE' => true,
        'PARTITION' => true,
        'ROLLUP' => true,
        'INDEX' => true,
        'KEY' => true,
        'FOR' => true,
        'LOCK' => true,
        'DUPLICATE' => true,
        'DELAYED' => true,
        'LOW_PRIORITY' => true,
        'TABLE' => true,
        'TABLES' => true,
    ];

    private static $cache = [];

    /**
     * @return SelectQuery|InsertQuery|UpdateQuery|TruncateQuery|DeleteQuery|DropTableQuery|ShowTablesQuery
     */
    public static function parse(string $sql)
    {
        if (\array_key_exists($sql, self::$cache)) {
            return self::$cache[$sql];
        }

        return self::$cache[$sql] = self::parseImpl($sql);
    }

    /**
     * @return SelectQuery|InsertQuery|UpdateQuery|TruncateQuery|DeleteQuery|DropTableQuery|ShowTablesQuery
     */
    private static function parseImpl(string $sql)
    {
        $tokens = (new SQLLexer())->lex($sql);
        $tokens = self::buildTokenListFromLexemes($tokens);
        $token = $tokens[0];
        if ($token->value === TokenType::PAREN) {
            $close = self::findMatchingParen(0, $tokens);
            $tokens = \array_slice($tokens, 1, $close - 1);
            $token = $tokens[0];
        }
        if ($token->type !== TokenType::CLAUSE && $token->value !== 'TRUNCATE') {
            throw new ParserException("Unexpected {$token->value}");
        }
        switch ($token->value) {
            case 'SELECT':
                $select = new SelectParser(0, $tokens, $sql);
                return $select->parse();
            case 'UPDATE':
                $update = new UpdateParser($tokens, $sql);
                return $update->parse();
            case 'DELETE':
                $delete = new DeleteParser($tokens, $sql);
                return $delete->parse();
            case 'INSERT':
                $insert = new InsertParser($tokens, $sql);
                return $insert->parse();
            case 'TRUNCATE':
                $truncate = new TruncateParser($tokens, $sql);
                return $truncate->parse();
            case 'DROP':
                $truncate = new DropParser($tokens, $sql);
                return $truncate->parse();
            case 'SHOW':
                $truncate = new ShowParser($tokens, $sql);
                return $truncate->parse();
            default:
                throw new ParserException("Unexpected {$token->value}");
        }
        throw new ParserException("Parse error: unexpected end of input");
    }

    /**
     * @param list<array{string,int}> $tokens
     *
     * @return array<int, Token>
     */
    private static function buildTokenListFromLexemes(array $tokens)
    {
        $out = [];
        $count = \count($tokens);

        // all parameters in PDO MySQL start at 1. I know, it's weird.
        $parameter_offset = 1;

        foreach ($tokens as $i => [$token, $start]) {
            $trimmed_token = \trim($token);

            if ($trimmed_token === '' || $trimmed_token === '.') {
                $k = \array_key_last($out);
                if ($k !== null) {
                    $previous = $out[$k];
                    $previous->raw .= $token;
                    $previous->value .= $trimmed_token;
                    $out[$k] = $previous;
                }
                continue;
            }

            if (\preg_match('/^[0-9\.]+$/', $token)) {
                $out[] = new Token(TokenType::NUMERIC_CONSTANT, $token, $token, $start);
                continue;
            }

            $first_char = $token[0];

            if ($first_char === '\'' || $first_char === '"') {
                $raw = $token;
                $token = \substr($token, 1, \strlen($token) - 2);
                $token = \preg_replace("/\\\\0/", "\0", \preg_replace("/\\\\([^%_rntbZ0])/", '\\1', $token));
                $out[] = new Token(TokenType::STRING_CONSTANT, $token, $raw, $start);
                continue;
            }

            if ($first_char === '`') {
                $raw = $token;

                if (\substr($token, -1) === '`') {
                    $token = \substr($token, 1, -1);
                } else {
                    if (\substr($token, -2) === '`.') {
                        $token = \substr($token, 1, -2) . '.';
                    }
                }

                $previous_key = \array_key_last($out);

                if ($previous_key !== null
                    && $out[$previous_key]->type === TokenType::IDENTIFIER
                    && substr($out[$previous_key]->value, -1) === '.'
                ) {
                    $out[$previous_key]->value .= $token;
                    $out[$previous_key]->raw .= $raw;
                    continue;
                }

                $out[] = new Token(TokenType::IDENTIFIER, $token, $raw, $start);
                continue;
            }

            if ($first_char === '(') {
                $out[] = new Token(TokenType::PAREN, $token, $token, $start);
                continue;
            }

            if ($token === '*') {
                $k = \array_key_last($out);

                if ($k === null) {
                    throw new ParserException("Parse error: unexpected *");
                }

                $previous = $out[$k];
                $out[$k] = $previous;

                $previous_type = $previous->type;

                if ($previous_type !== TokenType::NUMERIC_CONSTANT
                    && $previous_type !== TokenType::STRING_CONSTANT
                    && $previous_type !== TokenType::NULL_CONSTANT
                    && $previous_type !== TokenType::IDENTIFIER
                    && $previous->value !== ')'
                ) {
                    $out[] = new Token(TokenType::IDENTIFIER, $token, $token, $start);
                    continue;
                }

                if ($previous->type === TokenType::IDENTIFIER && \substr($previous->value, -1) === '.') {
                    $previous->value .= $token;
                    $previous->raw .= $token;
                    $out[$k] = $previous;
                    continue;
                }
            } elseif ($token === '-' || $token === '+') {
                $k = \array_key_last($out);

                if ($k === null) {
                    throw new ParserException("Parse error: unexpected {$token}");
                }

                $previous = $out[$k];

                $previous_type = $previous->type;

                if ($previous_type !== TokenType::NUMERIC_CONSTANT
                    && $previous_type !== TokenType::STRING_CONSTANT
                    && $previous_type !== TokenType::NULL_CONSTANT
                    && $previous_type !== TokenType::IDENTIFIER
                    && $previous->value !== ')'
                ) {
                    if ($token === '-') {
                        $op = 'UNARY_MINUS';
                    } else {
                        $op = 'UNARY_PLUS';
                    }

                    $out[] = new Token(TokenType::OPERATOR, $op, $token, $start);
                    continue;
                }
            }

            $token_upper = \strtoupper($token);

            if ($token_upper === 'LIKE'
                || $token_upper === 'REGEXP'
                || $token_upper === 'IN'
            ) {
                $previous_key = \array_key_last($out);

                if ($previous_key !== null
                    && $out[$previous_key]->value === 'NOT'
                ) {
                    $out[$previous_key]->value .= ' ' . $token_upper;
                    $out[$previous_key]->raw .= $token;

                    continue;
                }
            }

            if ($token_upper === '?') {
                $token_obj = new Token(TokenType::IDENTIFIER, $token, $token, $start);
                $token_obj->parameterOffset = $parameter_offset;
                $out[] = $token_obj;
                $parameter_offset++;
            } elseif ($token_upper === 'NULL') {
                $out[] = new Token(TokenType::NULL_CONSTANT, $token, $token, $start);
            } elseif ($token_upper === 'TRUE') {
                $out[] = new Token(TokenType::NUMERIC_CONSTANT, '1', $token, $start);
            } elseif ($token_upper === 'FALSE') {
                $out[] = new Token(TokenType::NUMERIC_CONSTANT, '0', $token, $start);
            } elseif (\array_key_exists($token_upper, self::CLAUSES)) {
                $out[] = new Token(TokenType::CLAUSE, $token_upper, $token, $start);
            } elseif (\array_key_exists($token_upper, self::OPERATORS)
                && ($token_upper !== 'MOD'
                    || $i >= $count - 1
                    || $tokens[$i + 1][0] !== '('
                )
            ) {
                $out[] = new Token(TokenType::OPERATOR, $token_upper, $token, $start);
            } elseif (\array_key_exists($token_upper, self::RESERVED_WORDS)) {
                $out[] = new Token(TokenType::RESERVED, $token_upper, $token, $start);
            } elseif ($token_upper === 'IF' && $i < $count - 1 && $tokens[$i + 1][0] !== '(') {
                $out[] = new Token(TokenType::RESERVED, $token_upper, $token, $start);
            } elseif (\array_key_exists($token_upper, self::SEPARATORS)) {
                $out[] = new Token(TokenType::SEPARATOR, $token_upper, $token, $start);
            } elseif ($i < $count - 1 && $tokens[$i + 1][0] === '(') {
                $out[] = new Token(TokenType::SQLFUNCTION, $token_upper, $token, $start);
            } elseif ($first_char === ':') {
                $out[] = $token_obj = new Token(TokenType::IDENTIFIER, '?', '?', $start);
                $token_obj->parameterName = \trim($token);
            } else {
                $previous_key = \array_key_last($out);
                if ($previous_key !== null && $out[$previous_key]->type === TokenType::IDENTIFIER
                    && substr($out[$previous_key]->value, -1) === '.'
                ) {
                    $out[$previous_key]->value .= $token;
                    continue;
                }
                $out[] = new Token(TokenType::IDENTIFIER, $token, $token, $start);
            }
        }
        return $out;
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return int
     */
    public static function findMatchingParen(int $pointer, array $tokens)
    {
        $paren_count = 0;
        $remaining_tokens = \array_slice($tokens, $pointer);
        $token_count = \count($remaining_tokens);
        foreach ($remaining_tokens as $i => $token) {
            if ($token->type === TokenType::PAREN) {
                $paren_count++;
            } else {
                if ($token->type === TokenType::SEPARATOR && $token->value === ')') {
                    $paren_count--;
                    if ($paren_count === 0) {
                        return $pointer + $i;
                    }
                }
            }
        }
        throw new ParserException("Unclosed parentheses at index {$pointer}");
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return int
     */
    public static function findMatchingEnd(int $pointer, array $tokens)
    {
        $paren_count = 0;
        $remaining_tokens = \array_slice($tokens, $pointer);
        $token_count = \count($remaining_tokens);
        foreach ($remaining_tokens as $i => $token) {
            if ($token->type === TokenType::OPERATOR
                && $token->value === 'CASE'
            ) {
                $paren_count++;
            } else {
                if ($token->type === TokenType::OPERATOR && $token->value === 'END') {
                    $paren_count--;
                    if ($paren_count === 0) {
                        return $pointer + $i;
                    }
                }
            }
        }
        throw new ParserException("Unclosed parentheses at index {$pointer}");
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return int
     */
    public static function skipIndexHints(int $pointer, array $tokens)
    {
        $next_pointer = $pointer + 1;
        $next = $tokens[$next_pointer] ?? null;

        while ($next !== null
            && $next->type === TokenType::RESERVED
            && ($next->value === 'USE' || $next->value === 'IGNORE' || $next->value === 'FORCE')
        ) {
            $pointer += 2;
            $hint_type = $next->value;
            $next = $tokens[$pointer] ?? null;
            if ($next === null || ($next->value !== 'INDEX' && $next->value !== 'KEY')) {
                throw new ParserException("Expected INDEX or KEY in index hint");
            }
            $pointer++;
            $next = $tokens[$pointer] ?? null;
            if ($next === null) {
                if ($hint_type === 'USE') {
                    $pointer--;
                    return $pointer;
                }
                throw new ParserException("Expected expected FOR or index list in index hint");
            }
            if ($next->value === 'FOR') {
                $pointer++;
                $next = $tokens[$pointer] ?? null;
                if ($next === null) {
                    throw new ParserException("Expected JOIN, ORDER BY, or GROUP BY after FOR in index hint");
                } else {
                    if ($next->value === 'JOIN') {
                        $pointer++;
                        $next = $tokens[$pointer] ?? null;
                    } else {
                        if ($next->value === 'GROUP' || $next->value === 'ORDER') {
                            $pointer++;
                            $next = $tokens[$pointer] ?? null;
                            if ($next === null || $next->value !== 'BY') {
                                throw new ParserException("Expected BY in index hint after GROUP or ORDER");
                            }
                            $pointer++;
                            $next = $tokens[$pointer] ?? null;
                        } else {
                            throw new ParserException(
                                "Expected JOIN, ORDER BY, or GROUP BY after FOR in index hint"
                            );
                        }
                    }
                }
            }
            if ($next === null || $next->type !== TokenType::PAREN) {
                if ($hint_type === 'USE') {
                    $pointer--;
                    return $pointer;
                }
                throw new ParserException("Expected index expression after index hint");
            }
            $closing_paren_pointer = SQLParser::findMatchingParen($pointer, $tokens);
            $arg_tokens = \array_slice($tokens, $pointer + 1, $closing_paren_pointer - $pointer - 1);
            if (!\count($arg_tokens)) {
                throw new ParserException("Expected at least one argument to index hint");
            }
            $count = 0;
            foreach ($arg_tokens as $arg) {
                $count++;
                if ($count % 2 === 1) {
                    if ($arg->type !== TokenType::IDENTIFIER) {
                        throw new ParserException("Expected identifier in index hint");
                    }
                } else {
                    if ($arg->value !== ',') {
                        throw new ParserException("Expected , or ) after index hint");
                    }
                }
            }
            $pointer = $closing_paren_pointer;
            $next_pointer = $pointer + 1;
            $next = $tokens[$next_pointer] ?? null;
        }
        return $pointer;
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return int
     */
    public static function skipLockHints(int $pointer, array $tokens)
    {
        $next_pointer = $pointer + 1;
        $next = $tokens[$next_pointer] ?? null;
        if ($next !== null && $next->type === TokenType::RESERVED) {
            if ($next->value === 'FOR') {
                $next_pointer++;
                $next = $tokens[$next_pointer] ?? null;
                if ($next === null) {
                    throw new ParserException("Expected keyword after FOR");
                }
                if ($next->value === 'UPDATE') {
                    return $pointer + 2;
                }
                throw new ParserException("Unexpected keyword {$next->value} after FOR");
            } else {
                if ($next->value === 'LOCK') {
                    $expected = ['IN', 'SHARE', 'MODE'];
                    foreach ($expected as $index => $keyword) {
                        $next = $tokens[$next_pointer + $index + 1] ?? null;
                        if ($next === null || $next->value !== $keyword) {
                            throw new ParserException("Unexpected keyword near LOCK");
                        }
                    }
                    return $pointer + 4;
                }
            }
        }
        return $pointer;
    }

    public static function bustCache(): void
    {
        self::$cache = [];
    }
}
