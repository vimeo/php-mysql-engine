<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\JoinType;
use Vimeo\MysqlEngine\Query\Expression\Expression;
use Vimeo\MysqlEngine\Query\Expression\SubqueryExpression;
use Vimeo\MysqlEngine\Parser\ParserException;

final class FromClause
{
    /**
     * @var array<
     *      int,
     *      array{
     *          name:string,
     *          subquery:SubqueryExpression,
     *          join_type:JoinType::*,
     *          join_operator:'ON'|'USING',
     *          alias:string,
     *          join_expression:null|Expression
     *      }
     *  >
     */
    public $tables = [];

    /**
     * @var bool
     */
    public $mostRecentHasAlias = false;

    /**
     * @param array{
     *        name:string,
     *        subquery:SubqueryExpression,
     *        join_type:JoinType::*,
     *        join_operator:'ON'|'USING',
     *        alias:string,
     *        join_expression:null|Expression
     * } $table
     *
     * @return void
     */
    public function addTable(array $table)
    {
        $this->tables[] = $table;
        $this->mostRecentHasAlias = false;
    }

    /**
     * @return void
     */
    public function aliasRecentExpression(string $name)
    {
        $k = \array_key_last($this->tables);
        if ($k === null || $this->mostRecentHasAlias) {
            throw new ParserException("Unexpected AS");
        }
        $this->tables[$k]['alias'] = $name;
        $this->mostRecentHasAlias = true;
    }
}
