<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Char extends CharacterColumn implements StringColumn, Defaultable
{
    use MySqlDefaultTrait;
}
