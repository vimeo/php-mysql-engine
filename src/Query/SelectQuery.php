<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\MultiOperand;
use Vimeo\MysqlEngine\Parser\ParserException;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\LimitClause;

final class SelectQuery
{
    /**
     * @var ?Expression
     */
    public $whereClause = null;

    /**
     * @var array<int, array{expression: Expression, direction: string}>|null
     */
    public $orderBy = null;

    /**
     * @var LimitClause|null
     */
    public $limitClause = null;

    /**
     * @var array<int, Expression>
     */
    public $selectExpressions = [];

    /**
     * @var ?FromClause
     */
    public $fromClause = null;

    /**
     * @var array<int, Expression>|null
     */
    public $groupBy = null;

    /**
     * @var ?Expression
     */
    public $havingClause = null;

    /**
     * @var array<int, array{type:MultiOperand::*, query:SelectQuery}>
     */
    public $multiQueries = [];

    /**
     * @var array
     */
    public $options = [];

    /**
     * @var bool
     */
    public $needsSeparator = false;

    /**
     * @var bool
     */
    public $mostRecentHasAlias = false;

    /**
     * @var string
     */
    public $sql;

    /**
     * @var int
     */
    public $start;

    public function __construct(string $sql, int $start)
    {
        $this->sql = $sql;
        $this->start = $start;
    }

    /**
     * @return void
     */
    public function addSelectExpression(Expression $expr)
    {
        if ($this->needsSeparator) {
            throw new ParserException("Unexpected expression!");
        }
        $this->selectExpressions[] = $expr;
        $this->needsSeparator = true;
        $this->mostRecentHasAlias = false;
    }

    /**
     * @return void
     */
    public function addOption(string $option)
    {
        $this->options[] = $option;
    }

    /**
     * @return void
     */
    public function aliasRecentExpression(string $name)
    {
        $k = \array_key_last($this->selectExpressions);
        if ($k === null || $this->mostRecentHasAlias) {
            throw new ParserException("Unexpected AS");
        }
        $this->selectExpressions[$k]->name = $name;
        $this->mostRecentHasAlias = true;
    }

    /**
     * @param MultiOperand::UNION|MultiOperand::UNION_ALL|MultiOperand::EXCEPT|MultiOperand::INTERSECT $type
     *
     * @return void
     */
    public function addMultiQuery($type, SelectQuery $query)
    {
        $this->multiQueries[] = ['type' => $type, 'query' => $query];
    }
}
