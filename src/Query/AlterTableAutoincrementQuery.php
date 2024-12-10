<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\Expression;

final class AlterTableAutoincrementQuery
{
    /**
     * @var string
     */
    public $table;
    /**
     * @var int
     */
    public $value;
    /**
     * @var string
     */
    public $sql;

    public function __construct(string $table, int $value, string $sql)
    {
        $this->table = $table;
        $this->value = $value;
        $this->sql = $sql;
    }
}
