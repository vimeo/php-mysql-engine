<?php
namespace Vimeo\MysqlEngine\Schema;

class Index
{
    /**
     * @var string
     */
    public $type;

    /**
     * @var array
     */
    public $columns;

    public function __construct(
        string $type,
        array $columns
    ) {
        $this->type = $type;
        $this->columns = $columns;
    }
}
