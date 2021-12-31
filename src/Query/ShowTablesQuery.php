<?php
namespace MysqlEngine\Query;

use MysqlEngine\Query\Expression\Expression;

final class ShowTablesQuery
{
    /**
     * @var string
     */
    public $pattern;

    /**
     * @var string
     */
    public $sql;

    public function __construct(string $pattern, string $sql)
    {
        $this->pattern = $pattern;
        $this->sql = $sql;
    }
}
