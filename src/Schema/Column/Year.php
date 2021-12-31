<?php
namespace MysqlEngine\Schema\Column;

class Year extends \MysqlEngine\Schema\Column implements ChronologicalColumn, DefaultTable
{
    use MySqlDefaultTrait;
    use EmptyConstructorTrait;

    public function getPhpType() : string
    {
        return 'string';
    }
}
