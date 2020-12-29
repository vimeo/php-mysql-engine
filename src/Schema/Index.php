<?php
namespace Vimeo\MysqlEngine\Schema;

class Index
{
    /**
     * @var string
     */
    public $type;

    public array $columns;

    public function __construct(
        string $type,
        array $columns
    ) {
        $this->type = $type;
        $this->columns = $columns;
    }
}
