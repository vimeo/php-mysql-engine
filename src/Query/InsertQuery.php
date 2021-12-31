<?php
namespace MysqlEngine\Query;

use MysqlEngine\Query\Expression\BinaryOperatorExpression;
use MysqlEngine\Query\Expression\Expression;

final class InsertQuery
{
    /**
     * @var string
     */
    public $table;

    /**
     * @var string
     */
    public $sql;

    /**
     * @var bool
     */
    public $ignoreDupes;

    /**
     * @var array<int, BinaryOperatorExpression>
     */
    public $updateExpressions = [];

    /**
     * @var array<int, string>
     */
    public $insertColumns = [];

    /**
     * @var array<int, array<int, Expression>>
     */
    public $values = [];

    /**
     * @var array<int, BinaryOperatorExpression>|null
     */
    public $setClause = null;

    public function __construct(string $table, string $sql, bool $ignoreDupes)
    {
        $this->table = $table;
        $this->sql = $sql;
        $this->ignoreDupes = $ignoreDupes;
    }
}
