<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\Token;
use MysqlEngine\TokenType;

final class NamedPlaceholderExpression extends Expression
{
    /**
     * @var string
     */
    public $parameterName;

    /**
     * @param Token $token
     */
    public function __construct(Token $token, string $parameter_name)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->parameterName = $parameter_name;
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
