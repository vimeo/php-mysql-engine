<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Query\Expression\Expression;
use MysqlEngine\Parser\ExpressionParser;
use MysqlEngine\Parser\ParserException;
use MysqlEngine\Parser\Token;
use MysqlEngine\TokenType;

final class ExistsOperatorExpression extends Expression
{
    /**
     * @var Expression|null
     */
    public $exists = null;

    /**
     * @var bool
     */
    public $negated = false;

    public function __construct(bool $negated, Token $token)
    {
        $this->negated = $negated;
        $this->name = '';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['EXISTS'];
        $this->operator = 'EXISTS';
        $this->type = TokenType::OPERATOR;
        $this->start = $token->start;
    }

    /**
     * @return void
     */
    public function negate()
    {
        $this->negated = true;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->exists !== null;
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        $this->exists = $expr;
    }
}
