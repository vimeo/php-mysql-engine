<?php
namespace Vimeo\MysqlEngine\Schema;

abstract class Column
{
    /**
     * @var bool
     */
    private $isNullable = true;

    public function isNullable() : bool
    {
        return $this->isNullable;
    }

    /**
     * @return static
     */
    public function setNullable(bool $is_nullable)
    {
        $this->isNullable = $is_nullable;
        return $this;
    }

    public function getNullablePhp() : string
    {
        return '->setNullable(' . ($this->isNullable() ? 'true' : 'false') . ')';
    }

    /**
     * @return 'int'|'string'|'float'|'null'|'boolean'
     */
    abstract public function getPhpType() : string;

    abstract public function getPhpCode() : string;
}
