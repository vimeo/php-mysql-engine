<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Varchar extends CharacterColumn implements StringColumn, Defaultable
{
    use StringColumnTrait;
    use MySqlDefaultTrait;
}
