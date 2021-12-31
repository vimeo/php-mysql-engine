<?php
namespace MysqlEngine\Schema\Column;

/**
 * Special class used when inferring from uninitialised temporary variables
 */
class NullColumn extends \MysqlEngine\Schema\Column
{
    use EmptyConstructorTrait;

    public function getPhpType() : string
    {
        return 'null';
    }
}
