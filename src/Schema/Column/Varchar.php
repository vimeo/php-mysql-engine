<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Varchar extends CharacterColumn implements StringColumn, DefaultTable
{
    use StringColumnTrait;
    use MySqlDefaultTrait;
}
