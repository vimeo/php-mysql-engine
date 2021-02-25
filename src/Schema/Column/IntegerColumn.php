<?php
namespace Vimeo\MysqlEngine\Schema\Column;

interface IntegerColumn
{
    public function setDefault($mysql_default) : void;

    public function autoIncrement() : void;

    public function isAutoIncrement() : bool;

    public function isUnsigned() : bool;

    public function getDisplayWidth() : int;
}
