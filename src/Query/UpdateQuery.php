<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Query\Expression\BinaryOperatorExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;

final class UpdateQuery extends Query
{
    /**
     * @var array{name:string, subquery:SubqueryExpression, join_type:JoinType::*, join_operator:'ON'|'USING', alias:string, join_expression:null|Expression}
     */
    public $updateClause;

    /**
     * @var string
     */
    public $sql;

    /**
     * @var array<int, BinaryOperatorExpression>
     */
    public array $setClause = [];

    /**
     * @param array{name:string, subquery:SubqueryExpression, join_type:JoinType::*, join_operator:'ON'|'USING', alias:string, join_expression:null|Expression} $updateClause
     */
    public function __construct(array $updateClause, string $sql)
    {
        $this->updateClause = $updateClause;
        $this->sql = $sql;
    }
}

