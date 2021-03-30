<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\TokenType;

final class SelectParser
{
    const CLAUSE_ORDER = [
        'SELECT' => 1,
        'FROM' => 2,
        'WHERE' => 3,
        'GROUP' => 4,
        'HAVING' => 5,
        'ORDER' => 6,
        'LIMIT' => 7
    ];

    /**
     * @var string
     */
    private $currentClause = 'SELECT';

    /**
     * @var int
     */
    private $pointer;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @var string
     */
    private $sql;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(int $pointer, array $tokens, string $sql)
    {
        $this->pointer = $pointer;
        $this->tokens = $tokens;
        $this->sql = $sql;
    }

    public function parse() : SelectQuery
    {
        // if the first part of this query is nested, we should be able to unwrap it safely
        while ($this->tokens[$this->pointer]->value === '(') {
            $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);

            $subquery_tokens = \array_slice(
                $this->tokens,
                $this->pointer + 1,
                $close - $this->pointer - 1
            );

            array_splice($this->tokens, $this->pointer, $close - $this->pointer + 1, $subquery_tokens);
        }

        if ($this->tokens[$this->pointer]->value !== 'SELECT') {
            throw new ParserException("Parser error: expected SELECT");
        }

        $query = $this->parseMainSelect();

        if (\array_key_exists($this->pointer, $this->tokens)) {
            $next = $this->tokens[$this->pointer] ?? null;
            while ($next !== null
                && ($next->value === 'UNION' || $next->value === 'INTERSECT' || $next->value === 'EXCEPT')
            ) {
                $type = $next->value;
                if ($next->value === 'UNION') {
                    $next_plus = $this->tokens[$this->pointer + 1];
                    if ($next_plus->value === 'ALL') {
                        $type = 'UNION_ALL';
                        $this->pointer++;
                    }
                }
                $this->pointer++;
                $select = new SelectParser($this->pointer, $this->tokens, $this->sql);
                $q = $select->parse();
                $this->pointer = $select->pointer;
                $query->addMultiQuery($type, $q);
                $next = $this->tokens[$this->pointer] ?? null;
            }
        }

        return $query;
    }

    private function parseMainSelect() : SelectQuery
    {
        $query = new SelectQuery($this->sql, $this->tokens[$this->pointer]->start);
        $this->pointer++;

        $count = \count($this->tokens);

        while ($this->pointer < $count) {
            $token = $this->tokens[$this->pointer];

            switch ($token->type) {
                case TokenType::NUMERIC_CONSTANT:
                case TokenType::NULL_CONSTANT:
                case TokenType::STRING_CONSTANT:
                case TokenType::OPERATOR:
                case TokenType::SQLFUNCTION:
                case TokenType::IDENTIFIER:
                case TokenType::PAREN:
                    if ($this->currentClause !== 'SELECT') {
                        throw new ParserException("Unexpected {$token->value}");
                    }

                    if ($query->needsSeparator) {
                        if (($token->type === TokenType::IDENTIFIER || $token->type === TokenType::STRING_CONSTANT)
                        && !$query->mostRecentHasAlias
                        ) {
                            $query->aliasRecentExpression($token->value);
                            break;
                        }

                        throw new ParserException("Expected comma between expressions near {$token->value}");
                    }

                    $expression_parser = new ExpressionParser($this->tokens, $this->pointer - 1);
                    $start = $this->pointer;
                    list($this->pointer, $expression) = $expression_parser->buildWithPointer();
                    if (!$expression instanceof ColumnExpression && !$expression instanceof ConstantExpression) {
                        $name = '';
                        $slice = \array_slice($this->tokens, $start, $this->pointer - $start + 1);
                        foreach ($slice as $t) {
                            $name .= $t->raw;
                        }
                        $expression->name = \trim($name);
                    }
                    $query->addSelectExpression($expression);
                    break;
                case TokenType::SEPARATOR:
                    if ($token->value === ',') {
                        if (!$query->needsSeparator) {
                            throw new ParserException("Unexpected ,");
                        }
                        $query->needsSeparator = false;
                    } else {
                        if ($token->value === ';') {
                            if ($this->pointer !== $count - 1) {
                                throw new ParserException("Unexpected tokens after semicolon");
                            }
                            return $query;
                        } else {
                            throw new ParserException("Unexpected {$token->value}");
                        }
                    }
                    break;
                case TokenType::CLAUSE:
                    if (\array_key_exists($token->value, self::CLAUSE_ORDER)
                    && self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token->value]
                    ) {
                        throw new ParserException("Unexpected {$token->value}");
                    }
                    $this->currentClause = $token->value;
                    switch ($token->value) {
                        case 'FROM':
                            $from = new FromParser($this->pointer, $this->tokens);
                            list($this->pointer, $fromClause) = $from->parse();
                            $query->fromClause = $fromClause;
                            break;
                        case 'WHERE':
                            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
                            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
                            $query->whereClause = $expression;
                            break;
                        case 'GROUP':
                            $this->pointer++;
                            $next = $this->tokens[$this->pointer] ?? null;
                            $expressions = [];

                            if ($next === null || $next->value !== 'BY') {
                                throw new ParserException("Expected BY after GROUP");
                            }
                            while (true) {
                                $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
                                $expression_parser->setSelectExpressions($query->selectExpressions);
                                list($this->pointer, $expression) = $expression_parser->buildWithPointer();
                                if ($expression instanceof ConstantExpression) {
                                    $position = (int) $expression->value - 1;
                                    $expression = $query->selectExpressions[$position] ?? null;
                                    if ($expression === null) {
                                        throw new ParserException(
                                            "Invalid positional reference {$position} in GROUP BY"
                                        );
                                    }
                                }
                                $expressions[] = $expression;
                                $next = $this->tokens[$this->pointer + 1] ?? null;
                                if ($next === null || $next->value !== ',') {
                                    break;
                                }
                                $this->pointer++;
                            }
                            $query->groupBy = $expressions;
                            break;
                        case 'ORDER':
                            $p = new OrderByParser($this->pointer, $this->tokens, $query->selectExpressions);
                            list($this->pointer, $query->orderBy) = $p->parse();
                            break;
                        case 'HAVING':
                            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
                            $expression_parser->setSelectExpressions($query->selectExpressions);
                            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
                            $query->havingClause = $expression;
                            break;
                        case 'LIMIT':
                            $p = new LimitParser($this->pointer, $this->tokens);
                            list($this->pointer, $query->limitClause) = $p->parse();
                            break;
                        case 'UNION':
                        case 'EXCEPT':
                        case 'INTERSECT':
                            return $query;

                        default:
                            throw new ParserException("Unexpected {$token->value}");
                    }
                    $this->pointer = SQLParser::skipLockHints($this->pointer, $this->tokens);
                    break;
                case TokenType::RESERVED:
                    switch ($token->value) {
                        case 'AS':
                            $this->pointer++;
                            $next = $this->tokens[$this->pointer] ?? null;
                            if ($next === null
                                || ($next->type !== TokenType::IDENTIFIER
                                    && $next->type !== TokenType::STRING_CONSTANT)
                            ) {
                                throw new ParserException("Expected alias name after AS");
                            }
                            $query->aliasRecentExpression($next->value);
                            break;
                        case 'DISTINCT':
                        case 'DISTINCTROW':
                        case 'ALL':
                        case 'HIGH_PRIORITY':
                        case 'SQL_CALC_FOUND_ROWS':
                        case 'SQL_SMALL_RESULT':
                        case 'SQL_BIG_RESULT':
                        case 'SQL_BUFFER_RESULT':
                        case 'SQL_CACHE':
                        case 'SQL_NO_CACHE':
                            if ($token->value === 'DISTINCTROW') {
                                $token->value = 'DISTINCT';
                            }
                            $query->addOption($token->value);
                            break;
                        default:
                            throw new ParserException("Unexpected {$token->value}");
                    }
                    break;
            }

            $this->pointer++;
        }

        return $query;
    }
}
