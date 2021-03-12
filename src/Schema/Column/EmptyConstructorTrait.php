<?php
namespace Vimeo\MysqlEngine\Schema\Column;

trait EmptyConstructorTrait
{
    public function getPhpCode() : string
    {
        $default = '';

        if ($this instanceof Defaultable && $this->hasDefault()) {
            if ($this->getDefault() === null) {
                $default = '->setDefault(null)';
            } else {
                $default = '->setDefault(\'' . $this->getDefault() . '\')';
            }
        }

        return '(new \\' . static::class . '())'
            . $default
            . $this->getNullablePhp();
    }
}
