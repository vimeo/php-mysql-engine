<?php
namespace Vimeo\MysqlEngine\Schema;

abstract class Column
{
    public bool $isNullable = true;

    /**
     * @return 'int'|'string'|'float'
     */
    abstract public function getPhpType() : string;
}
