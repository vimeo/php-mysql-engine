<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;

final class DeleteQuery extends Query
{
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
