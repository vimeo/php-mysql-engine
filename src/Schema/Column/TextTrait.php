<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

trait TextTrait
{
    public function getPhpCode() : string
    {
        $default = $this->getDefault() !== null ? '\'' . $this->getDefault() . '\'' : 'null';
        
        return '(new \\' . static::class . '('
            . ($this->character_set !== null && $this->collation !== null
                ? ', \'' . $this->character_set . '\'' . ', \'' . $this->collation . '\''
                : '')
            . '))'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
