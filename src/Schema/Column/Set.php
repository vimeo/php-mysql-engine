<?php
namespace MysqlEngine\Schema\Column;

class Set extends \MysqlEngine\Schema\Column implements DefaultTable
{
    use MySqlDefaultTrait;
    use HasOptionsTrait;
}
