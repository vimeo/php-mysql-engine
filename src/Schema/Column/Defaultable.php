<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface Defaultable
{
    /**
     * @return $this
     */
    public function setDefault($mysql_default);

    public function hasDefault() : bool;

    public function getDefault();
}
