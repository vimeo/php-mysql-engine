<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Time extends \Vimeo\MysqlEngine\Schema\Column implements ChronologicalColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function getPhpType() : string
    {
        return 'string';
    }
}
