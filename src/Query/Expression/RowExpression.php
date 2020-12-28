<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\TokenType;

final class RowExpression extends Expression
{
    /**
     * @var array<int, Expression>
     */
    public $elements;

    /**
     * @param array<int, Expression> $elements
     */
    public function __construct(array $elements)
    {
        $this->elements = $elements;
        $this->precedence = 0;
        $this->name = '';
        $this->type = TokenType::PAREN;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
