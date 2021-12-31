<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Parser\Token;
use MysqlEngine\TokenType;
use MysqlEngine\Processor\ProcessorException;

final class FunctionExpression extends Expression
{
    /**
     * @var string
     */
    public $functionName;

    /**
     * @var bool
     */
    protected $evaluatesGroups = true;

    /**
     * @var Token
     */
    public $token;

    /**
     * @var array<int, Expression>
     */
    public $args;

    /**
     * @var bool
     */
    public $distinct;

    /**
     * @param Token $token
     * @param array<int, Expression>                                $args
     */
    public function __construct(Token $token, array $args, bool $distinct)
    {
        $this->token = $token;
        $this->args = $args;
        $this->distinct = $distinct;
        $this->type = $token->type;
        $this->precedence = 0;
        $this->functionName = $token->value;
        $this->name = $token->value;
        $this->operator = (string) $this->type;
        $this->start = $token->start;
    }

    /**
     * @return string
     */
    public function functionName()
    {
        return $this->functionName;
    }

    public function hasAggregate() : bool
    {
        if ($this->functionName === 'COUNT'
            || $this->functionName === 'SUM'
            || $this->functionName === 'MIN'
            || $this->functionName === 'MAX'
            || $this->functionName === 'AVG'
        ) {
            return true;
        }

        foreach ($this->args as $arg) {
            if ($arg->hasAggregate()) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }

    /**
     * @return Expression
     */
    public function getExpr()
    {
        \assert(\count($this->args) === 1, 'expression must have one argument');
        return \reset($this->args);
    }
}
