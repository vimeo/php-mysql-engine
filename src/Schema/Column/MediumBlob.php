<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class MediumBlob extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(16777215, 'binary', '_bin');
    }
}
