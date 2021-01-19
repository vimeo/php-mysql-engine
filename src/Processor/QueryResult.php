<?php
namespace Vimeo\MysqlEngine\Processor;

use Vimeo\MysqlEngine\Schema\Column;

class QueryResult
{
    /** @var array<int, non-empty-array<string, mixed>> */
    public $rows;

    /** @var array<string, Column> */
    public $columns;

    /** @var non-empty-array<int, array<int, non-empty-array<string, mixed>>> */
    public $grouped_rows;

    public function __construct(array $rows, array $columns, ?array $grouped_rows = null)
    {
        $this->rows = $rows;
        $this->columns = $columns;
        $this->grouped_rows = $grouped_rows;
    }
}
