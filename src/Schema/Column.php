<?php
namespace Vimeo\MysqlEngine\Schema;

abstract class Column
{
    /**
     * @var bool
     */
    public $isNullable = true;

    /**
     * @return 'int'|'string'|'float'|'null'
     */
    abstract public function getPhpType() : string;
}
