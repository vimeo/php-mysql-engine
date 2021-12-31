<?php
namespace MysqlEngine\Query;

use MysqlEngine\Query\Expression\Expression;

final class DropTableQuery
{
    /**
     * @var string
     */
    public $table;

    /**
     * @var bool
     */
    public $if_exists;

    /**
     * @var string
     */
    public $sql;

    public function __construct(string $table, bool $if_exists, string $sql)
    {
        $this->table = $table;
        $this->sql = $sql;
        $this->if_exists = $if_exists;
    }
}
