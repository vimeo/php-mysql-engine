<?php
namespace Vimeo\MysqlEngine\Query;

use Vimeo\MysqlEngine\Schema\TableDefinition;
use Vimeo\MysqlEngine\Query\Expression\Expression;

abstract class Query
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
}
