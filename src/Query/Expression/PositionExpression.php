<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\Token;
use MysqlEngine\Processor\ProcessorException;
use MysqlEngine\TokenType;

final class PositionExpression extends Expression
{
    /**
     * @var int
     */
    public $position;

    public function __construct(int $position, Token $token)
    {
        $this->position = $position;
        $this->type = TokenType::IDENTIFIER;
        $this->precedence = 0;
        $this->name = (string) $position;
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
