<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface IntegerColumn
{
    /**
     * @return $this
     */
    public function setDefault($mysql_default);

    /**
     * @return $this
     */
    public function autoIncrement();

    public function isAutoIncrement() : bool;
}
