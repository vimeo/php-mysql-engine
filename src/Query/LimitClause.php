<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\ConstantExpression;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\NamedPlaceholderExpression;
use Vimeo\MysqlEngine\Query\Expression\QuestionMarkPlaceholderExpression;

final class LimitClause
{
    /** @var ConstantExpression|NamedPlaceholderExpression|QuestionMarkPlaceholderExpression|null */
    public $offset;

    /** @var ConstantExpression|NamedPlaceholderExpression|QuestionMarkPlaceholderExpression */
    public $rowcount;

    /**
     * @param ConstantExpression|NamedPlaceholderExpression|QuestionMarkPlaceholderExpression $offset
     * @param ConstantExpression|NamedPlaceholderExpression|QuestionMarkPlaceholderExpression $rowcount
     */
    public function __construct(?Expression $offset, Expression $rowcount)
    {
        $this->offset = $offset;
        $this->rowcount = $rowcount;
    }
}
