<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\Query\MysqlColumnType;

final class CastExpression extends Expression
{
    /**
     * @var Token
     */
    public $token;

    /**
     * @var Expression
     */
    public $expr;

    /**
     * @var MysqlColumnType
     */
    public $castType;

    /**
     * @param Token $tokens
     */
    public function __construct(Token $token, Expression $expr, MysqlColumnType $cast_type)
    {
        $this->token = $token;
        $this->type = $token->type;
        $this->precedence = 0;
        $this->operator = (string) $this->type;
        $this->expr = $expr;
        $this->castType = $cast_type;
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
