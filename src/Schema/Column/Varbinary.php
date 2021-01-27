<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Varbinary extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct(int $max_string_length)
    {
        parent::__construct($max_string_length, 'binary', '_bin');
    }
}
