<?php
namespace Vimeo\MysqlEngine\Query;

class CreateIndex
{
    public string $name;

    public string $type;

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
    public array $cols = [];

    public string $mode;

    public string $parser;

    public $more;

    public string $key_block_size;
}
