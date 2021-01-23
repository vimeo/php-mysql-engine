<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;
use Vimeo\MysqlEngine\Processor\ProcessorException;
use Vimeo\MysqlEngine\TokenType;

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
