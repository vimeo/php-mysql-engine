<?php
namespace MysqlEngine\Schema\Column;

class Enum extends \MysqlEngine\Schema\Column implements DefaultTable
{
    use MySqlDefaultTrait;
    use HasOptionsTrait;
}
