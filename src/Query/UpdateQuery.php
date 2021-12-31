<?php
namespace MysqlEngine\Query;

use MysqlEngine\JoinType;
use MysqlEngine\Query\Expression\BinaryOperatorExpression;
use MysqlEngine\Query\Expression\Expression;
use MysqlEngine\Query\Expression\SubqueryExpression;
use MysqlEngine\Query\LimitClause;

final class UpdateQuery
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
     * @var string
     */
    public $tableName;

    /**
     * @var string
     */
    public $sql;

    /**
     * @var array<int, BinaryOperatorExpression>
     */
    public $setClause = [];

    public function __construct(string $tableName, string $sql)
    {
        $this->tableName = $tableName;
        $this->sql = $sql;
    }
}
