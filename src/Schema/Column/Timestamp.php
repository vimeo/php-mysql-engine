<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Timestamp extends \Vimeo\MysqlEngine\Schema\Column implements ChronologicalColumn, Defaultable
{
    use MySqlDefaultTrait;
    use EmptyConstructorTrait;

    public function getPhpType() : string
    {
        return 'string';
    }
}
