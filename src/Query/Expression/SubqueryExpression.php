<?php
namespace MysqlEngine\Query\Expression;

use MysqlEngine\Query\SelectQuery;
use MysqlEngine\TokenType;
use MysqlEngine\Processor\SelectProcessor;

final class SubqueryExpression extends Expression
{
    /**
     * @var SelectQuery
     */
    public $query;

    /**
     * @var string
     */
    public $name;

    public function __construct(SelectQuery $query, string $name)
    {
        $this->query = $query;
        $this->name = $name;
        $this->precedence = 0;
        $this->type = TokenType::CLAUSE;
        $this->start = $query->start;
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
