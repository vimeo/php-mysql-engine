<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Binary extends CharacterColumn implements StringColumn, DefaultTable
{
    use MySqlDefaultTrait;
}
