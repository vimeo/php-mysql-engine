<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\Token;
use MysqlEngine\TokenType;

final class VariableExpression extends Expression
{
    /**
     * @var string
     */
    public $variableName;

    /**
     * @param Token $token
     */
    public function __construct(Token $token)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->variableName = substr($token->value, 1);
        $this->name = $this->variableName;
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
