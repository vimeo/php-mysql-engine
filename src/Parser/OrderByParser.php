<?php
namespace MysqlEngine\Parser;

use MysqlEngine\Query\Expression\Expression;
use MysqlEngine\Query\Expression\ConstantExpression;
use MysqlEngine\Query\Expression\PositionExpression;
use MysqlEngine\TokenType;

final class OrderByParser
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
     * @var array<int, Expression>|null
     */
    private $selectExpressions = null;

    /**
     * @param array<int, Token> $tokens
     * @param array<int, Expression>|null                                    $selectExpressions
     */
    public function __construct(int $pointer, array $tokens, ?array $selectExpressions = null)
    {
        $this->pointer = $pointer;
        $this->tokens = $tokens;
        $this->selectExpressions = $selectExpressions;
    }

    /**
     * @return array{int, array<int, array{expression:Expression, direction:'ASC'|'DESC'}>}
     */
    public function parse()
    {
        if ($this->tokens[$this->pointer]->value !== 'ORDER') {
            throw new ParserException("Parser error: expected ORDER");
        }
        $this->pointer++;
        $next = $this->tokens[$this->pointer] ?? null;
        $expressions = [];
        if ($next === null || $next->value !== 'BY') {
            throw new ParserException("Expected BY after ORDER");
        }
        while (true) {
            $expression_parser = new ExpressionParser($this->tokens, $this->pointer);
            if ($this->selectExpressions !== null) {
                $expression_parser->setSelectExpressions($this->selectExpressions);
            }
            list($this->pointer, $expression) = $expression_parser->buildWithPointer();
            if ($expression instanceof ConstantExpression) {
                $position = (int) $expression->value;
                $expression = new PositionExpression($position, $next);
            }
            $next = $this->tokens[$this->pointer + 1] ?? null;
            $sort_direction = 'ASC';
            if ($next !== null && ($next->value === 'ASC' || $next->value === 'DESC')) {
                $this->pointer++;
                $sort_direction = $next->value;
                $next = $this->tokens[$this->pointer + 1] ?? null;
            }
            $expressions[] = ['expression' => $expression, 'direction' => $sort_direction];
            if ($next === null || $next->value !== ',') {
                break;
            }
            $this->pointer++;
        }
        return [$this->pointer, $expressions];
    }
}
