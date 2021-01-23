<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\TokenType;

final class ParameterExpression extends Expression
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
