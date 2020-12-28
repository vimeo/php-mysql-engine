<?php
namespace Vimeo\MysqlEngine\Schema;

abstract class Column
{
    public bool $isNullable = true;

    /**
     * @return 'int'|'string'|'float'
     */
    public abstract function getPhpType() : string;
}
