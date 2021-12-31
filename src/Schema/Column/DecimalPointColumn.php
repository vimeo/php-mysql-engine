<?php
namespace MysqlEngine\Schema\Column;

abstract class DecimalPointColumn extends \MysqlEngine\Schema\Column implements NumberColumn, DefaultTable
{
    use NumberColumnTrait;
    use MySqlDefaultTrait;

    /**
     * @var int
     */
    protected $precision;

    /**
     * @var int
     */
    protected $scale;

    public function __construct(int $precision, int $scale)
    {
        $this->precision = $precision;
        $this->scale = $scale;
    }

    public function getMaxValue()
    {
        return INF;
    }

    public function getMinValue()
    {
        return $this->unsigned ? 0 : -INF;
    }

    public function getDecimalPrecision() : int
    {
        return $this->precision;
    }

    public function getDecimalScale() : int
    {
        return $this->scale;
    }

    public function getPhpType() : string
    {
        return 'float';
    }

    public function getPhpCode() : string
    {
        $default = '';
        $mysqlDefault = $this->getDefault();
        if ($this->hasDefault()) {
            $default = '->setDefault('
                . ($mysqlDefault === null
                    ? 'null'
                    : '\'' . $mysqlDefault . '\'')
                . ')';
        }

        return '(new \\' . static::class . '('
            . $this->precision
            . ', ' . $this->scale
            . '))'
            . $default
            . $this->getNullablePhp();
    }
}
