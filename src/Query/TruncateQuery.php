<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\Expression;

final class TruncateQuery
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
     * @var string
     */
    public $table;

    /**
     * @var string
     */
    public $sql;

    public function __construct(string $table, string $sql)
    {
        $this->table = $table;
        $this->sql = $sql;
    }
}
