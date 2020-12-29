<?php
namespace Vimeo\MysqlEngine\Schema;

abstract class Column
{
    /**
     * @var bool
     */
    public $isNullable = true;

    /**
     * @return 'int'|'string'|'float'
     */
    abstract public function getPhpType() : string;
}
