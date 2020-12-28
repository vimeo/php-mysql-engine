<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\SQLFakeRuntimeException;

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
     * @var array{type: string, value: string, raw: string}
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
     * @param array{type: TokenType::*, value: string, raw: string} $token
     * @param array<int, Expression>                                                                                                                                                                                                                                             $args
     */
    public function __construct(array $token, array $args, bool $distinct)
    {
        $this->token = $token;
        $this->args = $args;
        $this->distinct = $distinct;
        $this->type = $token['type'];
        $this->precedence = 0;
        $this->functionName = $token['value'];
        $this->name = $token['value'];
        $this->operator = (string) $this->type;
    }

    /**
     * @return string
     */
    public function functionName()
    {
        return $this->functionName;
    }

    /**
     * @return bool
     */
    public function isAggregate()
    {
        return \in_array($this->functionName, ['COUNT', 'SUM', 'MIN', 'MAX', 'AVG'], true);
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
