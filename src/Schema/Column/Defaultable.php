<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface Defaultable
{
    public function setDefault($mysql_default) : void;

    public function hasDefault() : bool;

    public function getDefault();
}
