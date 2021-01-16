<?php
namespace Vimeo\MysqlEngine\Schema\Column;

/**
 * Special class used when inferring from uninitialised temporary variables
 */
class NullColumn extends \Vimeo\MysqlEngine\Schema\Column
{
    public function getPhpType() : string
    {
        return 'string';
    }
}
