<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Query\Expression\Expression;

final class TruncateQuery
{
    public ?Expression $whereClause = null;

    /**
     * @var array<int, array{expression: Expression, direction: string}>|null
     */
    public ?array $orderBy = null;

    /**
     * @var array{rowcount:int, offset:int}|null
     */
    public ?array $limitClause = null;

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
