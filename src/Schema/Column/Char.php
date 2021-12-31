<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Char extends CharacterColumn implements StringColumn, DefaultTable
{
    use MySqlDefaultTrait;
}
