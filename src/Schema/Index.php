<?php
namespace Vimeo\MysqlEngine\Schema;

class Index
{
    /**
     * @var 'INDEX'|'UNIQUE'|'PRIMARY'|'FULLTEXT'|'SPATIAL'
     */
    public $type;

    /**
     * @var array
     */
    public $columns;

    /**
     * @param 'INDEX'|'UNIQUE'|'PRIMARY'|'FULLTEXT'|'SPATIAL' $type
     */
    public function __construct(
        string $type,
        array $columns
    ) {
        $this->type = $type;
        $this->columns = $columns;
    }
}
