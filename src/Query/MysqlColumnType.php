<?php
namespace Vimeo\MysqlEngine\Query;

class MysqlColumnType
{
    /**
     * @var string
     */
    public $type;

    /**
     * @var ?int
     */
    public $length;

    /**
     * @var ?int
     */
    public $decimals;

    /**
     * @var bool
     */
    public $unsigned = false;

    /**
     * @var array
     */
    public $values;

    /**
     * @var bool
     */
    public $null;

    /**
     * @var bool
     */
    public $zerofill = false;

    /**
     * @var string
     */
    public $character_set;

    /**
     * @var string
     */
    public $collation;
}
