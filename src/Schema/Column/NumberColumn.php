<?php
namespace MysqlEngine\Schema\Column;

interface NumberColumn
{
    /**
     * @return numeric
     */
    public function getMaxValue();

    /**
     * @return numeric
     */
    public function getMinValue();

    /** @return static */
    public function setDefault(?string $mysqlDefault);
}
