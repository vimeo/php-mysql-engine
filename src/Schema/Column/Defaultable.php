<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface Defaultable
{
    /**
     * @return static
     */
    public function setDefault($mysql_default);

    public function hasDefault() : bool;

    public function getDefault();
}
