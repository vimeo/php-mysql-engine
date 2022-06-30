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

    public function getPhpType(): string
    {
        return 'boolean';
    }

    public function getPhpCode(): string
    {
        $default = '';

        if ($this instanceof Defaultable && $this->hasDefault()) {
            $default = '->setDefault('
                . ($this->getDefault() === null
                    ? 'null'
                    : '\'' . $this->getDefault() . '\'')
                . ')';
        }

        $output = '(new \\' . static::class . '('
            . ($this->unsigned ? 'true' : 'false')
            . ', ' . $this->integer_display_width
            . '))'
            . $default
            . $this->getNullablePhp();

        var_export($output);
        return $output;
    }
}
