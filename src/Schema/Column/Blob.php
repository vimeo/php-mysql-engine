<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Blob extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(65535, 'binary', '_bin');
    }
}
