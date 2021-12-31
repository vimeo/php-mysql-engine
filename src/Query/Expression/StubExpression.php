<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\TokenType;
use MysqlEngine\Processor\ProcessorException;

final class StubExpression extends Expression
{
    public function __construct()
    {
        $this->precedence = 0;
        $this->name = '';
        $this->type = TokenType::RESERVED;
        $this->start = -1;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return false;
    }

    public function negate()
    {
        $this->negated = true;
    }
}
