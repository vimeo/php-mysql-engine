<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\JoinOperator;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\StubExpression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Query\FromClause;

final class FromParser
{
    /**
     * @var int
     */
    private $pointer;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(int $pointer, array $tokens)
    {
        $this->pointer = $pointer;
        $this->tokens = $tokens;
    }

    /**
     * @return array{0:int, 1:FromClause}
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'FROM') {
            throw new ParserException("Parser error: expected FROM");
        }
        $from = new FromClause();
        $this->pointer++;
        $count = \count($this->tokens);

        while ($this->pointer < $count) {
            $token = $this->tokens[$this->pointer];

            switch ($token->type) {
                case TokenType::STRING_CONSTANT:
                    if (!$from->mostRecentHasAlias) {
                        $from->aliasRecentExpression((string) $token->value);
                        $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                    } else {
                        throw new ParserException("Unexpected string constant {$token->raw}");
                    }

                    break;

                case TokenType::IDENTIFIER:
                    if (\count($from->tables) === 0) {
                        $table = ['name' => $token->value, 'join_type' => JoinType::JOIN];
                        $from->addTable($table);
                        $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                    } else {
                        if (!$from->mostRecentHasAlias) {
                            $from->aliasRecentExpression($token->value);
                            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                        }
                    }
                    break;

                case TokenType::SEPARATOR:
                    if ($token->value !== ',') {
                        throw new ParserException("Unexpected {$token->value}");
                    }

                    $this->pointer++;
                    $next = $this->tokens[$this->pointer] ?? null;

                    if ($next === null) {
                        throw new ParserException("Expected token after ,");
                    }

                    $table = $this->getTableOrSubquery($next);
                    $table['join_type'] = JoinType::CROSS;
                    $from->addTable($table);
                    $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                    break;

                case TokenType::CLAUSE:
                    return [$this->pointer - 1, $from];

                case TokenType::RESERVED:
                    switch ($token->value) {
                        case 'AS':
                            $this->pointer++;
                            $next = $this->tokens[$this->pointer] ?? null;
                            if ($next === null || $next->type !== TokenType::IDENTIFIER) {
                                throw new ParserException("Expected identifer after AS");
                            }
                            $from->aliasRecentExpression($next->value);
                            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
                            break;

                        case 'JOIN':
                        case 'INNER':
                        case 'LEFT':
                        case 'RIGHT':
                        case 'STRAIGHT_JOIN':
                        case 'NATURAL':
                        case 'CROSS':
                            $last = \end($from->tables);

                            if ($last === null) {
                                throw new ParserException("Parser error: unexpected join keyword");
                            }

                            $join = $this->buildJoin($last['name'], $token);
                            $from->addTable($join);
                            break;

                        default:
                            throw new ParserException("Unexpected {$token->value}");
                    }
                    break;

                case TokenType::PAREN:
                    $subquery = $this->getTableOrSubquery($token);
                    $from->addTable($subquery);
                    break;

                default:
                    throw new ParserException("Unexpected {$token->value}");
            }

            $this->pointer++;
        }

        return [$this->pointer, $from];
    }

    /**
     * @param Token $token
     *
     * @return array{
     *         name: string,
     *         subquery?: SubqueryExpression,
     *         join_type: JoinType::*,
     *         alias?: string
     *  }
     */
    private function getTableOrSubquery(Token $token)
    {
        switch ($token->type) {
            case TokenType::IDENTIFIER:
                return ['name' => $token->value, 'join_type' => JoinType::JOIN];
            case TokenType::PAREN:
                return $this->getSubquery();
            default:
                throw new ParserException("Expected table name or subquery");
        }
    }

    /**
     * @return array{
     *         name: string,
     *         subquery: SubqueryExpression,
     *         join_type: JoinType::*,
     *         alias: string
     *  }
     */
    private function getSubquery()
    {
        $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);

        $subquery_tokens = \array_slice(
            $this->tokens,
            $this->pointer + 1,
            $close - $this->pointer - 1
        );
        if (!\count($subquery_tokens)) {
            throw new ParserException("Empty parentheses found");
        }
        $this->pointer = $close;

        $subquery_sql = \implode(
            ' ',
            \array_map(
                function ($token) {
                    return $token->value;
                },
                $subquery_tokens
            )
        );
        $parser = new SelectParser(0, $subquery_tokens, $subquery_sql);
        $select = $parser->parse();
        $expr = new SubqueryExpression($select, '');
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
        if ($next !== null && $next->value === 'AS') {
            $this->pointer++;
            $next = $this->tokens[$this->pointer];
        }
        if ($next === null || $next->type !== TokenType::IDENTIFIER) {
            throw new ParserException("Every subquery must have an alias");
        }
        $name = $next->value;
        return [
            'name' => $name,
            'subquery' => $expr,
            'join_type' => JoinType::JOIN,
            'alias' => $name
        ];
    }

    /**
     * @param Token $token
     *
     * @return array{
     *         name: string,
     *         subquery: SubqueryExpression,
     *         join_type: JoinType::*,
     *         join_operator: string,
     *         alias: string,
     *         join_expression: Expression|null
     * }
     */
    private function buildJoin(string $left_table, Token $token)
    {
        $join_type = $token_value = $token->value;
        if ($token->value === 'INNER') {
            $join_type = 'JOIN';
        }
        if ($token_value === 'INNER' || $token_value === 'CROSS' || $token_value === 'NATURAL') {
            $this->pointer++;
            $next = $this->tokens[$this->pointer] ?? null;
            if ($next === null || $next->value !== 'JOIN') {
                throw new ParserException("Expected keyword JOIN after {$token->value}");
            }
        } else {
            if ($token_value === 'LEFT' || $token_value === 'RIGHT') {
                $this->pointer++;
                $next = $this->tokens[$this->pointer] ?? null;
                if ($next !== null && $next->value === 'OUTER') {
                    $this->pointer++;
                    $next = $this->tokens[$this->pointer] ?? null;
                }
                if ($next === null || $next->value !== 'JOIN') {
                    throw new ParserException("Expected keyword JOIN after {$token->value}");
                }
            }
        }
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;

        if ($next === null) {
            throw new ParserException("Expected table or subquery after join keyword");
        }
        $table = $this->getTableOrSubquery($next);
        $table['join_type'] = $join_type;
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
        if ($next === null) {
            return $table;
        }

        if ($next->type === TokenType::CLAUSE) {
            $this->pointer--;
            return $table;
        }

        if ($next->type === TokenType::IDENTIFIER) {
            $table['alias'] = $next->value;
            $this->pointer++;
            $next = $this->tokens[$this->pointer] ?? null;
            if ($next === null) {
                return $table;
            }
        } else {
            if ($next->value === 'AS') {
                $this->pointer++;
                $next = $this->tokens[$this->pointer] ?? null;
                if ($next === null || $next->type !== TokenType::IDENTIFIER) {
                    throw new ParserException("Expected identifier after AS");
                }
                $table['alias'] = $next->value;
                $this->pointer++;
                $next = $this->tokens[$this->pointer] ?? null;
                if ($next === null) {
                    return $table;
                }
            }
        }

        /*
         * Unlike other clauses (e.g., FROM), the buildJoin advances the pointer to the specified keyword (e.g., FORCE).
         * Therefore, the pointer needs to be adjusted.
         * For instance, in "FROM a FORCE INDEX ...", processing for other clauses ends just before the identifier (a),
         * but for the JOIN clause, the pointer advances to "FORCE".
         * To address this issue, we adjusted the pointer before and after calling SQLParser::skipIndexHints(),
         * and modified the code to advance the pointer to the closing parenthesis ')' if necessary.
         */
        $this->pointer--;
        $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
        $this->pointer++;
        if ($this->tokens[$this->pointer]->type === TokenType::SEPARATOR
            && $this->tokens[$this->pointer]->value === ")") {
            $this->pointer++;
        }
        $next = $this->tokens[$this->pointer] ?? null;
        if ($next === null) {
            /** @psalm-suppress LessSpecificReturnStatement */
            return $table;
        }

        if ($table['join_type'] === JoinType::NATURAL || $table['join_type'] === JoinType::CROSS) {
            return $table;
        }

        if ($next->type !== TokenType::RESERVED || ($next->value !== 'ON' && $next->value !== 'USING')) {
            $this->pointer--;
            return $table;
        }

        if ($next->value === 'USING') {
            $table['join_operator'] = 'USING';
            $this->pointer++;
            $next = $this->tokens[$this->pointer] ?? null;
            if ($next === null || $next->type !== TokenType::PAREN) {
                throw new ParserException("Expected ( after USING clause");
            }
            $closing_paren_pointer = SQLParser::findMatchingParen($this->pointer, $this->tokens);
            $arg_tokens = \array_slice($this->tokens, $this->pointer + 1, $closing_paren_pointer - $this->pointer - 1);
            if (!\count($arg_tokens)) {
                throw new ParserException("Expected at least one argument to USING() clause");
            }
            $count = 0;
            $filter = null;
            foreach ($arg_tokens as $arg) {
                $count++;
                if ($count % 2 === 1) {
                    if ($arg->type !== TokenType::IDENTIFIER) {
                        throw new ParserException("Expected identifier in USING clause");
                    }

                    $filter = self::addJoinFilterExpression(
                        $filter,
                        $left_table,
                        $table['name'],
                        $arg->value,
                        $arg->start
                    );
                } else {
                    if ($arg->value !== ',') {
                        throw new ParserException("Expected , after argument in USING clause");
                    }
                }
            }
            $this->pointer = $closing_paren_pointer + 1;
            $table['join_expression'] = $filter;
        } else {
            $table['join_operator'] = 'ON';
            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
            $table['join_expression'] = $expression;
        }
        return $table;
    }

    public static function addJoinFilterExpression(
        ?Expression $filter,
        string $left_table,
        string $right_table,
        string $column,
        int $start
    ) : BinaryOperatorExpression {
        $left = new ColumnExpression(
            new Token(TokenType::IDENTIFIER, "{$left_table}.{$column}", '', $start)
        );
        $right = new ColumnExpression(
            new Token(TokenType::IDENTIFIER, "{$right_table}.{$column}", '', $start)
        );
        $expr = new BinaryOperatorExpression($left, false, '=', $right);
        if ($filter !== null) {
            $filter = new BinaryOperatorExpression($filter, false, 'AND', $expr);
        } else {
            $filter = $expr;
        }
        return $filter;
    }
}
