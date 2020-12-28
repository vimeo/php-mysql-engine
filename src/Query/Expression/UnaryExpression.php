<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;
use Vimeo\MysqlEngine\TokenType;

final class UnaryExpression extends Expression
{
    /**
     * @var Expression|null
     */
    public $subject = null;

    /**
     * @var string
     */
    public $operator;

    public function __construct(string $operator)
    {
        $this->operator = $operator;
        $this->type = TokenType::OPERATOR;
        $this->precedence = 14;
        $this->name = $operator;
    }

    public function setNextChild(Expression $expr, bool $overwrite = false) : void
    {
        if ($this->subject !== null && !$overwrite) {
            throw new SQLFakeParseException("Unexpected expression after unary operand");
        }

        $this->subject = $expr;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return $this->subject !== null;
    }
}
