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
    public function __construct(Token $token)
    {
        $this->type = $token->type;
        $this->precedence = 0;
        $this->parameterName = \trim(\substr($token->raw, 1));
        $this->name = '?';
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
