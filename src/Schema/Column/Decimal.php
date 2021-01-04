<?php
namespace Vimeo\MysqlEngine\Schema\Column;

class Decimal extends DecimalPointColumn implements NumberColumn, Defaultable
{
    use NumberColumnTrait;
    use MySqlDefaultTrait;

    public function getMaxValue() : float
    {
        return pow(10, $this->precision - $this->scale) - pow(10, -$this->scale);
    }

    /**
     * @return int|float
     */
    public function getMinValue()
    {
        return $this->unsigned ? 0 : -$this->getMaxValue();
    }

    public function getPhpType() : string
    {
        return 'string';
    }
}
