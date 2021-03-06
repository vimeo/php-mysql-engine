<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;

final class QuestionMarkPlaceholderExpression extends Expression
{
    /**
     * @var int
     */
    public $offset;

    /**
     * @param Token $token
     */
    public function __construct(Token $token, int $offset)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->offset = $offset;
        $this->name = '?';
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
