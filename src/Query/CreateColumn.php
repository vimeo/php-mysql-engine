<?php
namespace Vimeo\MysqlEngine\Query;

class CreateColumn
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var string
     */
    public $type;

    /**
     * @var int
     */
    public $length;

    /**
     * @var int
     */
    public $decimals;

    /**
     * @var bool
     */
    public $unsigned;

    /**
     * @var bool
     */
    public $null;

    /**
     * @var string
     */
    public $default;

    /**
     * @var array
     */
    public $values;

    /**
     * @var bool
     */
    public $auto_increment;

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

    public $more;
}
