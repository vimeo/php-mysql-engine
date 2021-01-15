<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;

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
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
