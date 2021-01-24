<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\ParameterExpression;

final class LimitClause
{
    /** @var ConstantExpression|ParameterExpression|null */
    public $offset;

    /** @var ConstantExpression|ParameterExpression */
    public $rowcount;

    /**
     * @param ConstantExpression|ParameterExpression $offset
     * @param ConstantExpression|ParameterExpression $rowcount
     */
    public function __construct(?Expression $offset, Expression $rowcount)
    {
        $this->offset = $offset;
        $this->rowcount = $rowcount;
    }
}
