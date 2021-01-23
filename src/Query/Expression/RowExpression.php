<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
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
    public function __construct(array $elements, Token $token)
    {
        $this->elements = $elements;
        $this->precedence = 0;
        $this->name = '';
        $this->type = TokenType::PAREN;
        $this->start = $token->start;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
