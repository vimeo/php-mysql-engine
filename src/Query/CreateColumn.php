<?php
namespace Vimeo\MysqlEngine\Query;

class CreateColumn
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var MysqlColumnType
     */
    public $type;

    /**
     * @var string
     */
    public $default;

    /**
     * @var bool
     */
    public $auto_increment;

    public $more;
}
