<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;

final class UpdateQuery
{
    public ?Expression $whereClause = null;

    /**
     * @var array<int, array{expression: Expression, direction: string}>|null
     */
    public ?array $orderBy = null;

    /**
     * @var array{rowcount:int, offset:int}|null
     */
    public ?array $limitClause = null;

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
    public array $setClause = [];
    
    public function __construct(string $tableName, string $sql)
    {
        $this->tableName = $tableName;
        $this->sql = $sql;
    }
}
