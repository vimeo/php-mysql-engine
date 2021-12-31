<?php
namespace MysqlEngine\Query;

class CreateIndex
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var 'INDEX'|'UNIQUE'|'PRIMARY'|'FULLTEXT'|'SPATIAL'
     */
    public $type;

    /**
     * @var array<
     *     int,
     *     array{
     *         name:string,
     *         length:int,
     *         direction:string
     *     }
     * >
     */
    public $cols = [];

    /**
     * @var string
     */
    public $mode;

    /**
     * @var string
     */
    public $parser;

    public $more;

    /**
     * @var string
     */
    public $key_block_size;
}
