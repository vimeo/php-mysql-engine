<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class Text extends CharacterColumn implements StringColumn
{
    use TextTrait;
    use MySqlDefaultTrait;

    public function __construct(?string $character_set = null, ?string $collation = null)
    {
        parent::__construct(65535, $character_set, $collation);
    }
}
