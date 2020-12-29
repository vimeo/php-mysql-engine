<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;

final class DeleteQuery
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
     * @var array{rowcount:int, offset:int}|null
     */
    public $limitClause = null;

    /**
     * @var array{
     *      name:string,
     *      subquery:SubqueryExpression,
     *      join_type:JoinType::*,
     *      join_operator:'ON'|'USING',
     *      alias:string,
     *      join_expression:null|Expression
     * }|null
     */
    public $fromClause = null;

    /**
     * @var string
     */
    public $sql;

    public function __construct(string $sql)
    {
        $this->sql = $sql;
    }
}
