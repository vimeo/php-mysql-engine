<?php
namespace MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

trait TextTrait
{
    /**
     * @return string
     */
    public function getPhpCode() : string
    {
        $mysqlDefault = $this->getDefault();
        $default = $mysqlDefault !== null ? '\'' . $mysqlDefault . '\'' : 'null';
        
        return '(new \\' . static::class . '('
            . ($this->characterSet !== null && $this->collation !== null
                ? ', \'' . $this->characterSet . '\'' . ', \'' . $this->collation . '\''
                : '')
            . '))'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
