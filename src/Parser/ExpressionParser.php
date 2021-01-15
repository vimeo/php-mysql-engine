<?php
namespace Vimeo\MysqlEngine\Parser;

use Vimeo\MysqlEngine\Query\CreateColumn;
use Vimeo\MysqlEngine\Query\Expression\BetweenOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\CaseOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\CastExpression;
use Vimeo\MysqlEngine\Query\Expression\ColumnExpression;
use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\ExistsOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\FunctionExpression;
use Vimeo\MysqlEngine\Query\Expression\InOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\IntervalOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\PlaceholderExpression;
use Vimeo\MysqlEngine\Query\Expression\PositionExpression;
use Vimeo\MysqlEngine\Query\Expression\RowExpression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Query\Expression\UnaryExpression;
use Vimeo\MysqlEngine\Query\Expression\VariableExpression;
use Vimeo\MysqlEngine\TokenType;

final class ExpressionParser
{
    const OPERATOR_PRECEDENCE = [
        'INTERVAL' => 17,
        'BINARY' => 16,
        'COLLATE' => 16,
        '!' => 15,
        'UNARY_MINUS' => 14,
        'UNARY_PLUS' => 14,
        '~' => 14,
        '^' => 13,
        '*' => 12,
        '/' => 12,
        'DIV' => 12,
        '%' => 12,
        'MOD' => 12,
        '-' => 11,
        '+' => 11,
        '<<' => 10,
        '>>' => 10,
        '&' => 9,
        '|' => 8,
        '=' => 7,
        '<=>' => 7,
        '>=' => 7,
        '>' => 7,
        '<=' => 7,
        '<' => 7,
        '<>' => 7,
        '!=' => 7,
        'IS' => 7,
        'LIKE' => 7,
        'REGEXP' => 7,
        'IN' => 7,
        'EXISTS' => 7,
        'BETWEEN' => 6,
        'CASE' => 6,
        'WHEN' => 6,
        'THEN' => 6,
        'ELSE' => 6,
        'END' => 6,
        'NOT' => 5,
        'AND' => 4,
        '&&' => 4,
        'XOR' => 3,
        'OR' => 2,
        '||' => 2,
        ':=' => 1,
    ];

    /**
     * @var array<int, Expression>|null
     */
    private $selectExpressions = null;

    /**
     * @var array<int, Token>
     */
    private $tokens;

    /**
     * @var int
     */
    private $pointer = -1;

    /**
     * @var Expression
     */
    private $expression;

    /**
     * @var int
     */
    public $min_precedence = 0;

    /**
     * @var bool
     */
    private $is_child = false;

    /**
     * @param array<int, Token> $tokens
     */
    public function __construct(
        array $tokens,
        int $pointer = -1,
        ?Expression $expression = null,
        int $min_precedence = 0,
        bool $is_child = false
    ) {
        $this->tokens = $tokens;
        $this->pointer = $pointer;
        $this->expression = $expression ?: new PlaceholderExpression();
        $this->min_precedence = $min_precedence;
        $this->is_child = $is_child;
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return array{0: bool, 1: array<int, Expression>}
     */
    private function getListExpression(array $tokens)
    {
        $distinct = false;
        $pos = 0;
        $token_count = \count($tokens);
        $needs_comma = false;
        $args = [];

        while ($pos < $token_count) {
            $arg = $tokens[$pos];


            if ($arg->value === ',') {
                if ($needs_comma) {
                    $needs_comma = false;
                    $pos++;
                    continue;
                } else {
                    throw new SQLFakeParseException("Unexpected comma in SQL query");
                }
            }

            $p = new ExpressionParser($tokens, $pos - 1);
            list($pos, $expr) = $p->buildWithPointer();
            $args[] = $expr;
            $pos++;
            $needs_comma = true;
        }

        return [$distinct, $args];
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return array{Expression, non-empty-list<Token>}
     */
    private function getCastAsExpression(array $tokens)
    {
        $pos = 0;
        $token_count = \count($tokens);
        $needs_as = 0;
        $expr = null;
        $as_type_tokens = [];

        while ($pos < $token_count) {
            $arg = $tokens[$pos];

            if ($arg->value === 'AS') {
                if ($needs_as === 1) {
                    $needs_as = 2;
                    $pos++;
                    continue;
                } else {
                    throw new SQLFakeParseException("Unexpected AS in SQL query");
                }
            }

            if ($needs_as === 0) {
                $p = new ExpressionParser($tokens, $pos - 1);
                list($pos, $expr) = $p->buildWithPointer();
                $pos++;
                $needs_as = 1;
            } elseif ($needs_as === 2) {
                $as_type_tokens[] = $arg;
                $pos++;
            }
        }

        if (!$expr || !$as_type_tokens) {
            throw new SQLFakeParseException("Expecting two parts to CAST query");
        }

        return [$expr, $as_type_tokens];
    }

    /**
     * @param Token $token
     *
     * @return Expression
     */
    public function tokenToExpression(Token $token)
    {
        switch ($token->type) {
            case TokenType::NUMERIC_CONSTANT:
            case TokenType::STRING_CONSTANT:
            case TokenType::NULL_CONSTANT:
                return new ConstantExpression($token);

            case TokenType::IDENTIFIER:
                if ($this->selectExpressions !== null) {
                    foreach ($this->selectExpressions as $expr) {
                        if ($expr->name === $token->value) {
                            return $expr;
                        }
                    }
                }

                if ($token->value[0] === '@') {
                    return new VariableExpression($token);
                }

                return new ColumnExpression($token);

            case TokenType::SQLFUNCTION:
                $next = ($__tmp1__ = $this->nextToken()) !== null ? $__tmp1__ : (function () {
                    throw new \TypeError('Failed assertion');
                })();

                \assert($next->type === TokenType::PAREN, 'function is be followed by parentheses');
                $closing_paren_pointer = SQLParser::findMatchingParen($this->pointer, $this->tokens);
                $arg_tokens = \array_slice(
                    $this->tokens,
                    $this->pointer + 1,
                    $closing_paren_pointer - $this->pointer - 1
                );

                if ($token->value === 'CAST') {
                    [$expr, $as_type_tokens] = $this->getCastAsExpression($arg_tokens);

                    $as_type_token_values = array_map(
                        function ($token) {
                            return $token->value;
                        },
                        $as_type_tokens
                    );

                    $create_column = new CreateColumn();

                    $type = CreateTableParser::parseFieldType($as_type_token_values, true);

                    $fn = new CastExpression($token, $expr, $type);
                } else {
                    list($distinct, $args) = $this->getListExpression($arg_tokens);
                    $fn = new FunctionExpression($token, $args, $distinct);
                }

                $this->pointer = $closing_paren_pointer;

                return $fn;

            default:
                throw new SQLFakeNotImplementedException("Not implemented: {$token->value}");
        }
    }

    /**
     * @return Expression
     */
    public function build()
    {
        $token = $this->nextToken();
        $break_while = false;
        while ($token !== null) {
            switch ($token->type) {
                case TokenType::PAREN:
                    $close = SQLParser::findMatchingParen($this->pointer, $this->tokens);
                    $arg_tokens = \array_slice($this->tokens, $this->pointer + 1, $close - $this->pointer - 1);

                    if (!\count($arg_tokens)) {
                        throw new SQLFakeParseException("Empty parentheses found");
                    }

                    $this->pointer = $close;
                    $expr = new PlaceholderExpression();

                    if ($arg_tokens[0]->value === 'SELECT') {
                        $subquery_sql = \implode(
                            ' ',
                            \array_map(
                                function ($token) {
                                    return $token->value;
                                },
                                $arg_tokens
                            )
                        );

                        $parser = new SelectParser(0, $arg_tokens, $subquery_sql);
                        $select = $parser->parse();
                        $expr = new SubqueryExpression($select, '');
                    } else {
                        if ($this->expression instanceof InOperatorExpression) {
                            $pointer = -1;
                            $in_list = [];
                            $token_count = \count($arg_tokens);

                            while ($pointer < $token_count) {
                                $p = new ExpressionParser($arg_tokens, $pointer);
                                list($pointer, $expr) = $p->buildWithPointer();
                                $in_list[] = $expr;
                                if ($pointer + 1 >= $token_count) {
                                    break;
                                }
                                $pointer++;
                                $next = $arg_tokens[$pointer];
                                if ($next->value !== ',') {
                                    throw new SQLFakeParseException("Expected , in IN () list");
                                }
                            }

                            ($__tmp2__ = $this->expression) instanceof InOperatorExpression ? $__tmp2__ : (function () {
                                throw new \TypeError('Failed assertion');
                            })();

                            $this->expression->setInList($in_list);
                            break;
                        }

                        $second_token = $arg_tokens[1] ?? null;
                        if ($second_token !== null && $second_token->type === TokenType::SEPARATOR) {
                            list($distinct, $elements) = $this->getListExpression($arg_tokens);
                            if ($distinct) {
                                throw new SQLFakeParseException("Unexpected DISTINCT in row expression");
                            }
                            $expr = new RowExpression($elements);
                        } else {
                            $p = new ExpressionParser($arg_tokens, -1);
                            $expr = $p->build();
                        }
                    }

                    if ($this->expression instanceof PlaceholderExpression) {
                        $this->expression = new BinaryOperatorExpression($expr);
                    } else {
                        $this->expression->setNextChild($expr);
                    }

                    break;

                case TokenType::NUMERIC_CONSTANT:
                case TokenType::NULL_CONSTANT:
                case TokenType::STRING_CONSTANT:
                case TokenType::SQLFUNCTION:
                case TokenType::IDENTIFIER:
                    $expr = $this->tokenToExpression($token);

                    if ($this->expression instanceof PlaceholderExpression) {
                        $this->expression = new BinaryOperatorExpression($expr);
                    } elseif ($this->expression instanceof IntervalOperatorExpression
                        && $token->type === TokenType::IDENTIFIER
                        && $this->expression->number
                    ) {
                        $this->expression->setUnit($token->value);
                    } else {
                        if (($this->expression->operator === null || $this->expression->operator === '')
                            && $this->expression instanceof BinaryOperatorExpression
                            && $token->type === TokenType::IDENTIFIER
                        ) {
                            $this->pointer--;
                            return $this->expression->left;
                        }

                        $this->expression->setNextChild($expr);
                    }
                    break;

                case TokenType::OPERATOR:
                    $operator = $token->value;

                    if ($operator === 'CASE') {
                        $close = SQLParser::findMatchingEnd($this->pointer, $this->tokens);
                        $arg_tokens = \array_slice($this->tokens, $this->pointer + 1, $close - $this->pointer);

                        $p = new ExpressionParser($arg_tokens, -1, new CaseOperatorExpression());
                        $expr = $p->build();

                        $this->pointer = $close;

                        if ($this->expression instanceof PlaceholderExpression) {
                            $this->expression = new BinaryOperatorExpression($expr);
                        } else {
                            $this->expression->setNextChild($expr);
                        }

                        break;
                    }

                    if ($operator === 'INTERVAL') {
                        if (!$this->expression instanceof PlaceholderExpression) {
                            $this->pointer = $this->expression->addRecursiveExpression(
                                $this->tokens,
                                $this->pointer - 1
                            );
                            break;
                        }

                        $this->expression = new IntervalOperatorExpression();
                        break;
                    }

                    if ($operator === 'WHEN' || $operator === 'THEN' || $operator === 'ELSE' || $operator === 'END') {
                        if (!$this->expression instanceof CaseOperatorExpression) {
                            if ($this->expression instanceof BinaryOperatorExpression) {
                                $this->pointer--;
                                return $this->expression->left;
                            }

                            throw new SQLFakeParseException("Unexpected {$operator}");
                        }

                        $this->expression->setKeyword($operator);

                        if ($operator !== 'END') {
                            $this->pointer = $this->expression->addRecursiveExpression(
                                $this->tokens,
                                $this->pointer
                            );
                        }

                        break;
                    }

                    if ($this->expression->operator !== null && $this->expression->operator !== '') {
                        if ($operator === 'AND'
                            && $this->expression->operator === 'BETWEEN'
                            && !$this->expression->isWellFormed()
                        ) {
                            if (!$this->expression instanceof BetweenOperatorExpression) {
                                throw new \TypeError('Failed assertion');
                            }

                            $this->expression->foundAnd();
                        } else {
                            if ($operator === 'NOT') {
                                if ($this->expression->operator !== 'IS') {
                                    $next = $this->peekNext();
                                    if ($next !== null
                                        && (($next->type === TokenType::OPERATOR
                                            && (\strtoupper($next->value) === 'IN'
                                                || \strtoupper($next->value) === 'EXISTS'))
                                        || $next->type === TokenType::PAREN)
                                    ) {
                                        $this->pointer = $this->expression->addRecursiveExpression(
                                            $this->tokens,
                                            $this->pointer,
                                            true
                                        );
                                        break;
                                    }

                                    throw new SQLFakeParseException("Unexpected NOT");
                                }

                                $this->expression->negate();
                            } else {
                                $current_op_precedence = $this->expression->precedence;
                                $new_op_precedence = $this->getPrecedence($operator);

                                if ($current_op_precedence < $new_op_precedence) {
                                    $this->pointer = $this->expression->addRecursiveExpression(
                                        $this->tokens,
                                        $this->pointer - 1
                                    );
                                } else {
                                    if ($operator === 'BETWEEN') {
                                        $this->expression = new BetweenOperatorExpression(
                                            $this->expression
                                        );
                                    } else {
                                        if ($operator === 'IN') {
                                            $this->expression = new InOperatorExpression(
                                                $this->expression,
                                                $this->expression->negated
                                            );
                                        } elseif ($operator === 'NOT IN') {
                                            $this->expression = new InOperatorExpression(
                                                $this->expression,
                                                !$this->expression->negated
                                            );
                                        } else {
                                            $this->expression = new BinaryOperatorExpression(
                                                $this->expression,
                                                false,
                                                $operator
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        if ($operator === 'BETWEEN') {
                            if (!$this->expression instanceof BinaryOperatorExpression) {
                                throw new SQLFakeParseException('Unexpected keyword BETWEEN');
                            }

                            $this->expression = new BetweenOperatorExpression($this->expression->left);
                        } elseif ($operator === 'NOT') {
                            $this->expression->negate();
                        } elseif ($operator === 'IN' || $operator === 'NOT IN') {
                            if (!$this->expression instanceof BinaryOperatorExpression) {
                                throw new SQLFakeParseException('Unexpected keyword IN');
                            }

                            $this->expression = new InOperatorExpression(
                                $this->expression->left,
                                $operator === 'NOT IN' || $this->expression->negated
                            );
                        } elseif ($operator === 'EXISTS') {
                            $this->expression = new ExistsOperatorExpression(
                                $this->expression->negated
                            );
                        } elseif ($operator === 'UNARY_MINUS'
                            || $operator === 'UNARY_PLUS'
                            || $operator === '~'
                            || $operator === '!'
                        ) {
                            if (!$this->expression instanceof PlaceholderExpression) {
                                throw new \TypeError('Failed assertion');
                            }

                            $this->expression = new UnaryExpression($operator);
                        } else {
                            if (!$this->expression instanceof BinaryOperatorExpression) {
                                throw new \TypeError(
                                    'Expecting BinaryOperatorExpression but saw ' . \get_class($this->expression)
                                );
                            }

                            $this->expression->setOperator($operator);
                        }
                    }
                    break;

                default:
                    throw new SQLFakeParseException("Expression parse error: unexpected {$token->value}");
            }

            $nextToken = $this->peekNext();

            if (!$nextToken) {
                break;
            }

            if ($nextToken->type === TokenType::CLAUSE
                || $nextToken->type === TokenType::RESERVED
                || $nextToken->type === TokenType::SEPARATOR
            ) {
                if ($nextToken->value === 'VALUES' && !$this->expression->isWellFormed()) {
                    $this->tokens[$this->pointer + 1]->type = TokenType::SQLFUNCTION;
                } else {
                    break;
                }
            }

            if ($this->expression->isWellFormed()) {
                if ($nextToken->type === TokenType::IDENTIFIER) {
                    break;
                }
                if ($nextToken->value === 'ELSE'
                    || $nextToken->value === 'THEN'
                    || $nextToken->value === 'END'
                ) {
                    break;
                }
                if ($nextToken->type !== TokenType::OPERATOR) {
                    throw new SQLFakeParseException("Unexpected token {$nextToken->value}");
                }
                if ($this->is_child) {
                    $next_operator_precedence = $this->getPrecedence($nextToken->value);
                    if ($next_operator_precedence <= $this->min_precedence) {
                        break;
                    }
                }
            }

            $token = $this->nextToken();
        }

        if (!$this->expression->isWellFormed()) {
            if ($this->expression instanceof BinaryOperatorExpression && $this->expression->operator === '') {
                return $this->expression->left;
            }
            throw new SQLFakeParseException('Parse error, unexpected end of input for ' . \get_class($this->expression));
        }

        return $this->expression;
    }

    /**
     * @return array{0: int, 1: Expression}
     */
    public function buildWithPointer()
    {
        $expr = $this->build();
        return [$this->pointer, $expr];
    }

    /**
     * @return Token|null
     */
    private function nextToken()
    {
        $this->pointer++;
        return $this->tokens[$this->pointer] ?? null;
    }

    /**
     * @return Token|null
     */
    private function peekNext()
    {
        return $this->tokens[$this->pointer + 1] ?? null;
    }

    /**
     * @return int
     */
    private function getPrecedence(string $operator)
    {
        return self::OPERATOR_PRECEDENCE[$operator] ?? 0;
    }

    /**
     * @param array<int, Expression> $expressions
     *
     * @return void
     */
    public function setSelectExpressions(array $expressions)
    {
        $this->selectExpressions = $expressions;
    }
}
