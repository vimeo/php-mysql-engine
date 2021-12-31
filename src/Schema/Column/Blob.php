<?php
namespace MysqlEngine\Schema\Column;

class Blob extends CharacterColumn implements BlobColumn, DefaultTable
{
    use MySqlDefaultTrait;

    public function __construct()
    {
        parent::__construct(65535, 'binary', '_bin');
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
