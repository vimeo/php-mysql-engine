<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class LongBlob extends CharacterColumn implements BlobColumn, Defaultable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(4294967295, 'binary', '_bin');
    }

    public function getPhpCode() : string
    {
        $default = $this->getDefault() !== null ? '\'' . $this->getDefault() . '\'' : 'null';
        
        return '(new \\' . static::class . '())'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
