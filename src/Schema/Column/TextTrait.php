<?php
namespace Vimeo\MysqlEngine\Schema\Column;

use Pov\Definition\MySqlDefinition;

trait TextTrait
{
    public function getPhpCode() : string
    {
        $default = $this->getDefault() !== null ? '\'' . $this->getDefault() . '\'' : 'null';

        $args = [
            $this->character_set === null ? 'null' : "'{$this->character_set}'",
            $this->collation === null ? 'null' : "'{$this->collation}'",
        ];

        return '(new \\' . static::class . '(' . implode(', ', $args) . '))'
            . ($this->hasDefault() ? '->setDefault(' . $default . ')' : '')
            . $this->getNullablePhp();
    }
}
