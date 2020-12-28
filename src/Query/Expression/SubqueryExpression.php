<?php
namespace Vimeo\MysqlEngine\Query\Expression;

use Vimeo\MysqlEngine\Query\SelectQuery;
use Vimeo\MysqlEngine\TokenType;
use Vimeo\MysqlEngine\Processor\SelectProcessor;

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
    }

    /**
     * @return bool
     */
    public function isWellFormed()
    {
        return true;
    }
}
