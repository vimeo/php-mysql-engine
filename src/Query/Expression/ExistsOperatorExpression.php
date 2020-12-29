<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Parser\ExpressionParser;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\TokenType;

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

    public function __construct(bool $negated = false)
    {
        $this->negated = $negated;
        $this->name = '';
        $this->precedence = ExpressionParser::OPERATOR_PRECEDENCE['EXISTS'];
        $this->operator = 'EXISTS';
        $this->type = TokenType::OPERATOR;
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
