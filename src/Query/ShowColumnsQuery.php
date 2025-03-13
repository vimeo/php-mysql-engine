<?php

namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\Expression;

final class ShowColumnsQuery
{
    /**
     * @var bool
     */
    public $isFull = false;

    /**
     * @var ?Expression
     */
    public $whereClause = null;

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
