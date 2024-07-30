<?php

namespace Vimeo\MysqlEngine\Schema\Column;

class Boolean extends \Vimeo\MysqlEngine\Schema\Column implements NumberColumn, Defaultable
{
    use MySqlDefaultTrait;

    /**
     * @return int
     */
    public function getMaxValue()
    {
        return 1;
    }

    /**
     * @return int
     */
    public function getMinValue()
    {
        return 0;
    }

    /**
     * @return 'boolean'
     */
    public function getPhpType(): string
    {
        return 'boolean';
    }

    /**
     * @return string
     */
    public function getPhpCode() : string
    {
        $default = '';

        if ($this->hasDefault()) {
            $default = '->setDefault('
                . ($this->getDefault() === null
                    ? 'null'
                    : '\'' . $this->getDefault() . '\'')
                . ')';
        }

        return '(new \\' . static::class . '('
            . '))'
            . $default
            . $this->getNullablePhp();
    }
}
