<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class TinyBlob extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(255, 'binary', '_bin');
    }
}
