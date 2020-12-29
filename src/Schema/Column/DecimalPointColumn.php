<?php
namespace Vimeo\MysqlEngine\Schema\Column;

abstract class DecimalPointColumn extends \Vimeo\MysqlEngine\Schema\Column implements NumberColumn, Defaultable
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
}
