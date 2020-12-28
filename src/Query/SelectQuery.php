<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\MultiOperand;
use Vimeo\MysqlEngine\Parser\SQLFakeParseException;
use Vimeo\MysqlEngine\Query\Expression\Expression;

final class SelectQuery extends Query
{
    /**
     * @var array<int, Expression>
     */
    public array $selectExpressions = [];

    public ?FromClause $fromClause = null;

    /**
     * @var array<int, Expression>|null
     */
    public ?array $groupBy = null;

    public ?Expression $havingClause = null;

    /**
     * @var array<int, array{type:MultiOperand::*, query:SelectQuery}>
     */
    public array $multiQueries = [];

    public array $options = [];

    public bool $needsSeparator = false;

    public bool $mostRecentHasAlias = false;

    public string $sql;

    public function __construct(string $sql)
    {
        $this->sql = $sql;
    }

    /**
     * @return void
     */
    public function addSelectExpression(Expression $expr)
    {
        if ($this->needsSeparator) {
            throw new SQLFakeParseException("Unexpected expression!");
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
            throw new SQLFakeParseException("Unexpected AS");
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

