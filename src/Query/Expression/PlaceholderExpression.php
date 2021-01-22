<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\ProcessorException;

final class PlaceholderExpression extends Expression
{
    public function __construct()
    {
        $this->precedence = 0;
        $this->name = '';
        $this->type = TokenType::RESERVED;
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
