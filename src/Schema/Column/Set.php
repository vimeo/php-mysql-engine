<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Set extends \Vimeo\MysqlEngine\Schema\Column implements Defaultable
{
    use MySqlDefaultTrait;
    use HasOptionsTrait;
}
