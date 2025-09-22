<?php

namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Parser\Token;

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
    /** @var ?array<int, array{expression: Expression, direction: 'ASC'|'DESC'}> $order */
    public $order;
    /** @var ?Expression $separator */
    public $separator;

    /**
     * @param Token $token
     * @param array<int, Expression> $args
     * @param ?array<int, array{expression: Expression, direction: 'ASC'|'DESC'}> $order
     */
    public function __construct(
        Token  $token,
        array  $args,
        bool   $distinct,
        ?array  $order,
        ?Expression $separator
    )
    {
        $this->token = $token;
        $this->args = $args;
        $this->distinct = $distinct;
        $this->type = $token->type;
        $this->precedence = 0;
        $this->functionName = $token->value;
        $this->name = $token->value;
        $this->operator = $this->type;
        $this->start = $token->start;
        $this->separator = $separator;
        $this->order = $order;
    }

    /**
     * @return string
     */
    public function functionName()
    {
        return $this->functionName;
    }

    public function hasAggregate(): bool
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
