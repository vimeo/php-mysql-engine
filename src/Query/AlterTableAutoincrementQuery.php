<?php
namespace MysqlEngine\Query;

use MysqlEngine\Query\Expression\Expression;

final class AlterTableAutoincrementQuery
{
    /**
     * @var string
     */
    public $table;
    /**
     * @var string
     */
    public $value;
    /**
     * @var string
     */
    public $sql;

    /**
     * AlterTableAutoincrementQuery constructor.
     * @param string $table
     * @param string $value
     * @param string $sql
     */
    public function __construct(string $table, string $value, string $sql)
    {
        $this->table = $table;
        $this->value = $value;
        $this->sql = $sql;
    }
}