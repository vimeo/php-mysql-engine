<?php
namespace Vimeo\MysqlEngine\Schema\Column;

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

    public function setDefault($mysql_default) : void;
}
