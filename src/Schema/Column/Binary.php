<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Binary extends CharacterColumn implements StringColumn, Defaultable
{
    use MySqlDefaultTrait;
}
