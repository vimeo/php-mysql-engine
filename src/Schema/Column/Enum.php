<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Enum extends \Vimeo\MysqlEngine\Schema\Column implements Defaultable
{
    use MySqlDefaultTrait;
    use HasOptionsTrait;
}
