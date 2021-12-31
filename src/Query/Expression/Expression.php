<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\ParserException;
use MysqlEngine\TokenType;
use MysqlEngine\Parser\Token;

abstract class Expression
{
    /**
     * @var string
     */
    public $operator = '';

    /**
     * @var bool
     */
    public $negated = false;

    /**
     * @var int
     */
    public $precedence;

    /**
     * @var string
     */
    public $name;

    /**
     * @var string
     */
    protected $type;

    /**
     * @var bool
     */
    protected $evaluates_groups = false;

    /**
     * @var ?\MysqlEngine\Schema\Column
     */
    public $column;

    /**
     * @var int
     * @readonly
     */
    public $start;

    /**
     * @return void
     */
    public function negate()
    {
        throw new ParserException("Parse error: unexpected NOT for expression {$this->type}");
    }

    /**
     * @return bool
     */
    abstract public function isWellFormed();

    /**
     * @return TokenType::*
     */
    public function getType()
    {
        return $this->type;
    }

    public function setNextChild(self $expr, bool $overwrite = false) : void
    {
        throw new ParserException("Parse error: unexpected expression");
    }

    /**
     * @param array<int, Token> $tokens
     *
     * @return int
     */
    public function addRecursiveExpression(array $tokens, int $pointer, bool $negated = false) : int
    {
        throw new ParserException("Parse error: unexpected recursive expression");
    }

    public function hasAggregate() : bool
    {
        return false;
    }
}
