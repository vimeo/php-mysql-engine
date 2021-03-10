<?php
namespace Vimeo\MysqlEngine\Query;

final class ShowIndexQuery
{
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
