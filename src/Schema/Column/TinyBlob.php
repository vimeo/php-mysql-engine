<?php
namespace MysqlEngine\Schema\Column;

class TinyBlob extends CharacterColumn implements BlobColumn, DefaultTable
{
    use MySqlDefaultTrait;

    /**
     * TinyBlob constructor.
     */
    public function __construct()
    {
        parent::__construct(255, 'binary', '_bin');
    }

    /**
     * @return string
     */
    public function getPhpCode() : string
    {
        $mysqlDefault = $this->getDefault();
        $default = $mysqlDefault !== null ? '\'' . $mysqlDefault . '\'' : 'null';
        
        return '(new \\' . static::class . '())'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
