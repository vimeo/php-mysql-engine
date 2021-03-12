<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface IntegerColumn
{
    /** @return static */
    public function setDefault($mysql_default);

    /**
     * @return static
     */
    public function autoIncrement();

    public function isAutoIncrement() : bool;

    public function isUnsigned() : bool;

    public function getDisplayWidth() : int;
}
