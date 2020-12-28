<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class LongBlob extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(4294967295, 'binary', '_bin');
    }
}
