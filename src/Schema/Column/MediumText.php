<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class MediumText extends CharacterColumn
{
    use TextTrait;
    use MySqlDefaultTrait;

    /**
     * @var string
     */
    protected $type = 'mediumtext';

    public function __construct(?string $character_set = null, ?string $collation = null)
    {
        parent::__construct(16777215, $character_set, $collation);
    }
}
