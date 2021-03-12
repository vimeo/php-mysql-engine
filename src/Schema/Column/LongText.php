<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

class LongText extends CharacterColumn implements StringColumn
{
    use TextTrait;
    use MySqlDefaultTrait;

    /**
     * @var string
     */
    protected $type = 'longtext';

    public function __construct(?string $character_set = null, ?string $collation = null)
    {
        parent::__construct(4294967295, $character_set, $collation);
    }
}
