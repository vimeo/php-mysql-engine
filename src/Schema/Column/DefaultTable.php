<?php

namespace MysqlEngine\Schema\Column;

/**
 * Interface DefaultTable
 * @package MysqlEngine\Schema\Column
 */
interface DefaultTable
{
    /**
     * @return static
     */
    public function setDefault(?string $mysqlDefault);

    /**
     * @return bool
     */
    public function hasDefault() : bool;

    /**
     * @return string|null
     */
    public function getDefault() : ?string;
}
