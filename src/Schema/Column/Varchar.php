<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Varchar extends CharacterColumn implements StringColumn, Defaultable
{
    use StringColumnTrait;
    use MySqlDefaultTrait;

    public function __construct(int $max_string_length, ?string $character_set = null, ?string $collation = null)
    {
        parent::__construct($max_string_length, $character_set, $collation);
    }
}
