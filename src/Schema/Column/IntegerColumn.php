<?php
namespace MysqlEngine\Schema\Column;

interface IntegerColumn
{
    /** @return static */
    public function setDefault(?string $mysqlDefault);

    /**
     * @return static
     */
    public function autoIncrement();

    public function isAutoIncrement() : bool;

    public function isUnsigned() : bool;

    public function getDisplayWidth() : int;
}
