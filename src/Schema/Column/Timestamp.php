<?php
namespace MysqlEngine\Schema\Column;

class Timestamp extends \MysqlEngine\Schema\Column implements ChronologicalColumn, DefaultTable
{
    use MySqlDefaultTrait;
    use EmptyConstructorTrait;

    public function getPhpType() : string
    {
        return 'string';
    }
}
