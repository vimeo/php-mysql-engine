<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Year extends \Vimeo\MysqlEngine\Schema\Column implements ChronologicalColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function getPhpType() : string
    {
        return 'string';
    }
}
