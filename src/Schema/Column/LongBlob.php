<?php
namespace MysqlEngine\Schema\Column;

class LongBlob extends CharacterColumn implements BlobColumn, DefaultTable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(4294967295, 'binary', '_bin');
    }

    public function getPhpCode() : string
    {
        $mysqlDefault = $this->getDefault();
        $default = $mysqlDefault !== null ? '\'' . $mysqlDefault . '\'' : 'null';
        
        return '(new \\' . static::class . '())'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
